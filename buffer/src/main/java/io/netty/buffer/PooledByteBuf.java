/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import io.netty.util.internal.ObjectPool.Handle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * 这个类继承于 AbstractReferenceCountedByteBuf，其对象主要由内存
 * 池分配器PooledByteBufAllocator创建。比较常用的实现类有两种：
 * 一种是基于堆外直接内存池构建的PooledDirectByteBuf，是Netty在
 * 进行I/O的读/写时的内存分配的默认方式，堆外直接内存可以减少内
 * 存 数 据 拷 贝 的 次 数 ； 另 一 种 是 基 于 堆 内 内 存 池 构 建 的
 * PooledHeapByteBuf。
 *
 * 除 了 上 述 两 种 实 现 类 ， Netty 还 使 用 Java 的 后 门 类
 * sun.misc.Unsafe实现了两个缓冲区，即PooledUnsafeDirectByteBuf
 * 和PooledUnsafeHeapByteBuf。这个强大的后门类会暴露对象的底层地
 * 址，一般不建议使用，Netty为了优化性能引入了Unsafe。
 *
 * PooledUnsafeDirectByteBuf会暴露底层DirectByteBuffer的地址
 * memoryAddress，而Unsafe则可通过memoryAddress+Index的方式取得
 * 对应的字节。
 *
 * PooledUnsafeHeapByteBuf也会暴露字节数组在Java堆中的地址，
 * 因此不再使用字节数组的索引，即array[index]。同样，Unsafe可通
 * 过BYTE_ARRAY_BASE_OFFSET+Index字节的地址获取字节。
 *
 * 由于创建PooledByteBuf对象的开销大，而且在高并发情况下，当
 * 网络I/O进行读/写时会创建大量的实例。因此，为了降低系统开销，
 * Netty对Buffer对象进行了池化，缓存了Buffer对象，使对此类型的
 * Buffer可进行重复利用。PooledByteBuf是从内存池中分配出来的
 * Buffer，因此它需要包含内存池的相关信息，如内存块Chunk、
 * PooledByteBuf在内存块中的位置及其本身所占空间的大小等。
 *
 * Netty在默认情况下采用的是池化的 PooledByteBuf ，以提高程序
 * 性能。但是 PooledByteBuf 在使用完毕后需要手动释放，否则会因
 * PooledByteBuf 申请的内存空间没有归还导致内存泄漏，最终使内存溢出。
 * 为了解决这个问题，Netty运用JDK的弱引用和引用队列设计了一
 * 套专门的内存泄漏检测机制，用于实现对需要手动释放的ByteBuf对象
 * 的监控。
 *
 * 强引用：经常使用的编码方式，如果将一个对象赋值给一个变
 * 量，只要这个变量可用，那么这个对象的值就被该变量强引用了；否
 * 则垃圾回收器不会回收该对象。
 *
 * 软引用：当内存不足时，被垃圾回收器回收，一般用于缓存。
 *
 * 弱引用：只要是发生回收的时候，纯弱引用的对象都会被回收；
 * 当对象未被回收时，弱引用可以获取引用的对象。
 *
 * 虚引用：在任何时候都可能被垃圾回收器回收。如果一个对象与
 * 虚引用关联，则该对象与没有引用与之关联时一样。虚引用获取不到
 * 引用的对象。
 *
 * 引用队列：与虚引用或弱引用配合使用，当普通对象被垃圾回收
 * 器回收时，会将对象的弱引用和虚引用加入引用队列中。Netty运用这
 * 一特性来检测这些被回收的ByteBuf是否已经释放了内存空间。下面对
 * 其实现原理及源码进行详细剖析。
 * @param <T>
 */
abstract class PooledByteBuf<T> extends AbstractReferenceCountedByteBuf {

    //对象重复利用无须每次创建
    private final Handle<PooledByteBuf<T>> recyclerHandle;

    //一块大的内存区域
    protected PoolChunk<T> chunk;
    //定位到chunk中的一块连续内存的指针
    protected long handle;
    //chunk中具体的缓存空间
    protected T memory;
    //偏移量
    protected int offset;
    //长度(ByteBuf中的可读字节数)
    protected int length;
    //最大可用长度
    int maxLength;
    //线程缓存
    PoolThreadCache cache;
    //临时ByteBuffer 转换成ByteBuffer对象
    ByteBuffer tmpNioBuf;
    //内存分配器
    private ByteBufAllocator allocator;

    @SuppressWarnings("unchecked")
    protected PooledByteBuf(Handle<? extends PooledByteBuf<T>> recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        this.recyclerHandle = (Handle<PooledByteBuf<T>>) recyclerHandle;
    }

    void init(PoolChunk<T> chunk, ByteBuffer nioBuffer,
              long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        init0(chunk, nioBuffer, handle, offset, length, maxLength, cache);
    }

    void initUnpooled(PoolChunk<T> chunk, int length) {
        init0(chunk, null, 0, 0, length, length, null);
    }

    private void init0(PoolChunk<T> chunk, ByteBuffer nioBuffer,
                       long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        assert handle >= 0;
        assert chunk != null;
        assert !PoolChunk.isSubpage(handle) || chunk.arena.size2SizeIdx(maxLength) <= chunk.arena.smallMaxSizeIdx:
                "Allocated small sub-page handle for a buffer size that isn't \"small.\"";

        chunk.incrementPinnedMemory(maxLength);
        //大内存块默认为16MB，被分配给多个PooledByteBuf
        this.chunk = chunk;
        //chunk中具体的缓存空间
        memory = chunk.memory;
        //将PooledByteBuf转换成ByteBuffer
        tmpNioBuf = nioBuffer;
        //内存分配器：PooledByteBuf是由Arena的分配器构建的
        allocator = chunk.arena.parent;
        //线程缓存：优先从线程缓存中获取
        this.cache = cache;
        //通过这个指针可以得到PooledByteBuf在chunk这颗二叉树中的具体位置
        this.handle = handle;
        //偏移量
        this.offset = offset;
        //长度：实际数据长度
        this.length = length;
        //写指针不能超过PooledByteBuf的最大可用长度
        this.maxLength = maxLength;
    }

    /**
     * Method must be called before reuse this {@link PooledByteBufAllocator}
     */
    final void reuse(int maxCapacity) {
        maxCapacity(maxCapacity);
        resetRefCnt();
        setIndex0(0, 0);
        discardMarks();
    }

    @Override
    public final int capacity() {
        return length;
    }

    @Override
    public int maxFastWritableBytes() {
        return Math.min(maxLength, maxCapacity()) - writerIndex;
    }

    /**
     * 自动扩容
     * @param newCapacity 新的容量值
     * @return
     */
    @Override
    public final ByteBuf capacity(int newCapacity) {
        //若新的容量值增长与长度相等，则无须扩容，直接返回即可
        if (newCapacity == length) {
            ensureAccessible();
            return this;
        }
        //检查新的容量值是否大于最大允许容量
        checkNewCapacity(newCapacity);
        /**
         * 非内存池，在新容量值小于最大长度值的情况下，无须重新分配
         * 只需修改索引和数据长度即可。
         */
        if (!chunk.unpooled) {
            // If the request capacity does not require reallocation, just update the length of the memory.
            /**
             * 新的容量值大于长度值
             * 在没有超过Buffer的最大可用长度值时，只需要把长度设置为新的容量值即可
             * 若超过了最大可用长度值，则只能重新分配
             */
            if (newCapacity > length) {
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            } else if (newCapacity > maxLength >>> 1 &&
                    (maxLength > 512 || newCapacity > maxLength - 16)) {
                // here newCapacity < length
                // 当新容量值小于最大可用长度值时其读/写索引不能超过新容量值
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }
        }

        // Reallocation required.需要重新定位。
        chunk.decrementPinnedMemory(maxLength);
        //由Arena重新分配内存并释放旧的内存空间
        chunk.arena.reallocate(this, newCapacity, true);
        return this;
    }

    @Override
    public final ByteBufAllocator alloc() {
        return allocator;
    }

    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    @Override
    public final ByteBuf retainedDuplicate() {
        return PooledDuplicatedByteBuf.newInstance(this, this, readerIndex(), writerIndex());
    }

    @Override
    public final ByteBuf retainedSlice() {
        final int index = readerIndex();
        return retainedSlice(index, writerIndex() - index);
    }

    @Override
    public final ByteBuf retainedSlice(int index, int length) {
        return PooledSlicedByteBuf.newInstance(this, this, index, length);
    }

    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        //只有当tmpNioBuf为空时，才创建新的共享缓冲区
        if (tmpNioBuf == null) {
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        } else {
            tmpNioBuf.clear();
        }
        return tmpNioBuf;
    }

    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    /**
     * 对象回收，把对象属性清空
     */
    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            chunk.decrementPinnedMemory(maxLength);
            //释放内存
            chunk.arena.free(chunk, tmpNioBuf, handle, maxLength, cache);
            tmpNioBuf = null;
            chunk = null;
            recycle();
        }
    }

    /**
     * 把 PooledByteBuf放回对象池Stack中，以便下次使用
     */
    private void recycle() {
        recyclerHandle.recycle(this);
    }

    protected final int idx(int index) {
        return offset + index;
    }

    final ByteBuffer _internalNioBuffer(int index, int length, boolean duplicate) {
        //获取读索引
        index = idx(index);
        //当duplicate为true时，在memory中创建共享此缓冲区内容的新的字节缓冲区
        //当duplicate为false时，先从tmpNioBuf中获取，当tmpNioBuf为空时
        //再调用 newInternalNioBuffer，此处与memory的类型有关，因此其具体实现由子类完成
        // newInternalNioBuffer 衍生一个ByteBuffer,与原ByteBuffer对象共享内存。也即：
        // 并没有新的byte[] 数组产生。
        ByteBuffer buffer = duplicate ? newInternalNioBuffer(memory) : internalNioBuffer();
        //设置新的缓冲区指针位置及limit
        buffer.limit(index + length).position(index);
        return buffer;
    }

    /**
     * 从 memory中创建一份缓存ByteBuffer
     * 与 memory共享底层数据，但读/写索引独立维护
     * @param index
     * @param length
     * @return
     */
    ByteBuffer duplicateInternalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, true);
    }

    @Override
    public final ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, false);
    }

    @Override
    public final int nioBufferCount() {
        return 1;
    }

    @Override
    public final ByteBuffer nioBuffer(int index, int length) {
        return duplicateInternalNioBuffer(index, length).slice();
    }

    @Override
    public final ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    @Override
    public final boolean isContiguous() {
        return true;
    }

    /**
     * channel 从PooledByteBuf中获取数据
     * PooledByteBuf的读索引的变化
     * 由父类 AbstractByteBuf 的 readBytes() 方法维护
     * @param index
     * @param out
     * @param length the maximum number of bytes to transfer
     *
     * @return
     * @throws IOException
     */
    @Override
    public final int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length));
    }

    @Override
    public final int readBytes(GatheringByteChannel out, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false));
        readerIndex += readBytes;
        return readBytes;
    }

    /**
     * channel 从PooledByteBuf中获取数据
     * PooledByteBuf的读索引的变化
     *
     * @param index
     * @param out
     * @param position the file position at which the transfer is to begin
     * @param length the maximum number of bytes to transfer
     *
     * @return
     * @throws IOException
     */
    @Override
    public final int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length), position);
    }

    @Override
    public final int readBytes(FileChannel out, long position, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false), position);
        readerIndex += readBytes;
        return readBytes;
    }

    /**
     * 从Channel 中读取数据写入PooledByteBuf中
     * writeIndex 由父类AbstractByteBuf 的writeBytes() 方法维护
     * @param index
     * @param in
     * @param length the maximum number of bytes to transfer
     *
     * @return
     * @throws IOException
     */
    @Override
    public final int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length));
        } catch (ClosedChannelException ignored) {
            //客户端主动关闭连接，返回-1，触发对应的用户事件
            return -1;
        }
    }

    /**
     * 通过handler可以计算page的偏移量，也可以计算subpage的段在
     * page中的相对偏移量，两者加起来就是该段分配的内存在chunk中的相
     * 对位置偏移量。当PoolByteBuf从Channel中读取数据时，需要用这个
     * 相对位置偏移量来获取ByteBuffer。PoolByteBuf从原Buffer池中通过
     * duplicate()方法在不干扰原来Buffer索引的情况下，与其共享缓冲区
     * 数据，复制一份新的ByteBuffer，并初始化新ByteBuffer的元数据，
     * 通过offset指定读/写位置，用limit来限制读/写范围。
     *
     * @param index
     * @param in
     * @param position the file position at which the transfer is to begin
     * @param length the maximum number of bytes to transfer
     *
     * @return
     * @throws IOException
     */
    @Override
    public final int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length), position);
        } catch (ClosedChannelException ignored) {
            //客户端主动关闭连接，返回-1，触发对应的用户事件
            return -1;
        }
    }
}
