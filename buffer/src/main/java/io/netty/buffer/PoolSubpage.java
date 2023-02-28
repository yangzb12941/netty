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

import static io.netty.buffer.PoolChunk.RUN_OFFSET_SHIFT;
import static io.netty.buffer.PoolChunk.SIZE_SHIFT;
import static io.netty.buffer.PoolChunk.IS_USED_SHIFT;
import static io.netty.buffer.PoolChunk.IS_SUBPAGE_SHIFT;
import static io.netty.buffer.SizeClasses.LOG2_QUANTUM;

/**
 * 对于内存小于8KB的情况，Netty又是如何设计的呢？PoolChunk有一个
 * PoolSubpage[]组数，叫subpages，它的数组长度与PoolChunk的page
 * 节点数一致，都是2048，由于它只分配小于8KB的数据，且PoolChunk
 * 的每个page节点只能分配一种PoolSubpage，因此subpages的下标与
 * PoolChunk的page节点的偏移位置是一一对应的。PoolChunk上的每个
 * page均可分配一个PoolSubpage链表，由于链表中元素的大小都相同，
 * 因此这个链表最大的元素个数也已经确定了，链表的头节点为
 * PoolSubpage[]组数中的元素。
 *
 * PoolChunk分配PoolSubpage的步骤如下。
 * 步骤1：在PoolChunk的二叉树上找到匹配的节点，由于小于8KB，
 * 因此只匹配 page节点。因为page节点从2048开始，所以page将节点与
 * 2048进行异或操作就可以得到PoolSubpage[]数组subpages的下标
 * subpageIdx。
 *
 * 步骤2：取出subpages[subpageIdx]的值。若值为空，则新建一个
 * PoolSubpage并初始化。
 *
 * 步骤3：PoolSubpage根据内存大小分两种，即小于512B为tiny，
 * 大于或等于512B且小于8KB为small，此时PoolSubpage还未区分是
 * small还是tiny。然后把PoolSubpage加入PoolArena的PoolSubpage缓
 * 存池中，以便后续直接从缓存池中获取。
 *
 * 步骤4：PoolArena的PoolSubpage[]数组缓存池有两种，分别是存
 * 储(0,512)个字节的tinySubpagePools和存储[512,8192)个字节的
 * smallSubpagePools 。 由 于 所 有 内 存 分 配 大 小 elemSize 都 会 经 过
 * normalizeCapacity的处理，因此当elemSize>=512时，elemSize会成
 * 倍 增 长 ， 即 512→1024→2048→4096→8192； 当 elemSize<512 时 ，
 * elemSize 从 16 开 始 ， 每 次 加 16B ， elemSize 的 变 化 规 律 与
 * tinySubpagePools和smallSubpagePools两个数组的存储原理是一致的。
 * tinysubpage通过elemSize>>>4即可获取tinysubpage[]数组的下
 * 标tableIdx，而smallSubpage则需要除1024看是否为0，若不为0，则
 * 通过循环除以2再看是否为0，最终找到tableIdx。有了tableIdx就可
 * 以获取对应的PoolSubpage链表，若PoolSubpage链表获取失败，或链
 * 表中不可再分配元素，则回到步骤1，从PoolChunk中重新分配；否则
 * 直接获取链表中可分配的元素。
 *
 * PoolSubpage是由PoolChunk的page生成的，page可以生成多种
 * PoolSubpage ， 但 一 个 page 只 能 生 成 其 中 一 种 PoolSubpage 。
 * PoolSubpage可以分为很多段，每段的大小相同，且由申请的内存大小决定。
 *
 * @param <T>
 */
final class PoolSubpage<T> implements PoolSubpageMetric {

    //由于PoolSubpage每段的最小值为16B，因此它的段的总数量最多
    //为pageSize/16。把PoolSubpage中每段的内存使用情况用一个long[]
    //数组来标识，long类型的存储位数最大为64B，每一位用0表示为空闲
    //状态，用1表示被占用，这个数组的长度为pageSize/16/64B，默认情
    //况下，long数组的长度最大为8192/16/64B=8B。每次在分配内存时，
    //只需查找可分配二进制的位置，即可找到内存在page中的相对偏移
    //量，图6-7为 PoolSubpag 的内存段分配实例。
    // PoolSubpage 最多有8196/16=512段，需要用8个long类型(64b)的数字来描述这512个
    // 内存段的状态，1表示被占用，0表示空闲。每一个段用一个b来标识。
    // [0][0][0]...[0][0][0][0][1][1][1]
    //当前分配内存的chunk
    final PoolChunk<T> chunk;
    //切分后，每段的大小
    final int elemSize;
    //当前page在chunk的memoryMap中的下标id
    private final int pageShifts;
    //当前page在chunk的memory上的偏移量
    private final int runOffset;
    //page的大小，默认是8192
    private final int runSize;
    //PoolSubpage 每段内存的占用状态，采用二进制位来标识
    private final long[] bitmap;

    //指向前一个PoolSubpage
    PoolSubpage<T> prev;
    //指向后一个PoolSubpage
    PoolSubpage<T> next;

    boolean doNotDestroy;
    //段的总数量
    private int maxNumElems;
    //实际采用二进制位标识的long数组的长度值，根据每段大小elemSize和pageSize计算的来
    private int bitmapLength;
    //下一个可用的位置
    private int nextAvail;
    //可用的段的数量
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage() {
        chunk = null;
        pageShifts = -1;
        runOffset = -1;
        elemSize = -1;
        runSize = -1;
        bitmap = null;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int pageShifts, int runOffset, int runSize, int elemSize) {
        this.chunk = chunk;
        this.pageShifts = pageShifts;
        this.runOffset = runOffset;
        this.runSize = runSize;
        this.elemSize = elemSize;
        bitmap = new long[runSize >>> 6 + LOG2_QUANTUM]; // runSize / 64 / QUANTUM

        doNotDestroy = true;
        if (elemSize != 0) {
            maxNumElems = numAvail = runSize / elemSize;
            nextAvail = 0;
            bitmapLength = maxNumElems >>> 6;
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;
            }

            for (int i = 0; i < bitmapLength; i ++) {
                bitmap[i] = 0;
            }
        }
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     *
     * 通过handler可以计算page的偏移量，也可以计算subpage的段在
     * page中的相对偏移量，两者加起来就是该段分配的内存在chunk中的相
     * 对位置偏移量。当PoolByteBuf从Channel中读取数据时，需要用这个
     * 相对位置偏移量来获取ByteBuffer。PoolByteBuf从原Buffer池中通过
     * duplicate()方法在不干扰原来Buffer索引的情况下，与其共享缓冲区
     * 数据，复制一份新的ByteBuffer，并初始化新ByteBuffer的元数据，
     * 通过offset指定读/写位置，用limit来限制读/写范围。
     *
     * 查看 PooledByteBuf 关键代码：
     */
    long allocate() {
        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }
        //获取PoolSubpage下一个可用的位置
        final int bitmapIdx = getNextAvail();
        // 获取该位置在bitmap数组对应的下标
        int q = bitmapIdx >>> 6;
        //获取bitmap[q]上实际可用的位
        int r = bitmapIdx & 63;
        //确定该位没有被占用
        assert (bitmap[q] >>> r & 1) == 0;
        //将该位设置为1表示已被占用，此处1L<<r表示将r位设为1
        bitmap[q] |= 1L << r;
        //若没有可用的段，则说明此page/PoolSubpage 已经分配满了，没有必要继续放到PoolArena池中
        //应从pool中移除。
        if (-- numAvail == 0) {
            removeFromPool();
        }
        //把当前page的索引和PoolSubPage的索引一起返回
        //低32位表示page 的index，高32位表示PoolSubPage的index
        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            addToPool(head);
            /* When maxNumElems == 1, the maximum numAvail is also 1.
             * Each of these PoolSubpages will go in here when they do free operation.
             * If they return true directly from here, then the rest of the code will be unreachable
             * and they will not actually be recycled. So return true only on maxNumElems > 1. */
            if (maxNumElems > 1) {
                return true;
            }
        }

        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    //查找下一个可用的位置
    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        //若下一个可用位置大于或等于0，则说明是第一次分配或正好已经有内存回收，可直接返回
        if (nextAvail >= 0) {
            //每次分配完内存后，都要将nextAvail设置为-1
            this.nextAvail = -1;
            return nextAvail;
        }
        //没有直接可用内存，需要继续查找
        return findNextAvail();
    }

    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        final int bitmapLength = this.bitmapLength;
        //遍历用来标识内存是否被占用的数组
        for (int i = 0; i < bitmapLength; i ++) {
            long bits = bitmap[i];
            //若当前long型的标识位不全为1，则表示其中有未被使用的内存。
            if (~bits != 0) {
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        //i 表示 bitmap 的位置，由于bitmap每个值有64位
        //因此用i*64来表示bitmap[i]的第一位在PoolSubpage中的
        //偏移量
        final int baseVal = i << 6;

        for (int j = 0; j < 64; j ++) {
            //判断第一位是否为0，为0表示该为空闲
            if ((bits & 1) == 0) {
                int val = baseVal | j;
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            //若 bits的第一位不为0，则继续右移一位，判断第二位
            bits >>>= 1;
        }
        //若没有找到，则返回-1
        return -1;
    }

    /**
     * 把当前page的索引和PoolSubpage的索引一起返回
     * 低32位表示page的index,高32位表示PoolSubpage的index
     * @param bitmapIdx
     * @return
     */
    private long toHandle(int bitmapIdx) {
        int pages = runSize >> pageShifts;
        return (long) runOffset << RUN_OFFSET_SHIFT
               | (long) pages << SIZE_SHIFT
               | 1L << IS_USED_SHIFT
               | 1L << IS_SUBPAGE_SHIFT
               | bitmapIdx;

        //等同于 return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        if (chunk == null) {
            // This is the head so there is no need to synchronize at all as these never change.
            doNotDestroy = true;
            maxNumElems = 0;
            numAvail = 0;
            elemSize = -1;
        } else {
            synchronized (chunk.arena) {
                if (!this.doNotDestroy) {
                    doNotDestroy = false;
                    // Not used for creating the String.
                    maxNumElems = numAvail = elemSize = -1;
                } else {
                    doNotDestroy = true;
                    maxNumElems = this.maxNumElems;
                    numAvail = this.numAvail;
                    elemSize = this.elemSize;
                }
            }
        }

        if (!doNotDestroy) {
            return "(" + runOffset + ": not in use)";
        }

        return "(" + runOffset + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + runSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        if (chunk == null) {
            // It's the head.
            return -1;
        }

        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return 1 << pageShifts;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
