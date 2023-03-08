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
package io.netty.channel.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.FileRegion;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.internal.ChannelUtils;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.util.internal.StringUtil;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import static io.netty.channel.internal.ChannelUtils.WRITE_STATUS_SNDBUF_FULL;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on bytes.
 *
 * AbstractNioChannel拥有NIO的Channel，具备NIO的注册、连接等
 * 功能。但I/O的读/写交给了其子类，Netty对I/O的读/写分为POJO对象
 * 与ByteBuf和FileRegion，因此在AbstractNioChannel的基础上继续抽
 * 象 了 一 层 ， 分 为 AbstractNioMessageChannel 与 AbstractNioByteChannel 。
 *
 *  AbstractNioByteChannel 发 送 和 读 取 的 对 象 是 ByteBuf 与 FileRegion类型。
 */
public abstract class AbstractNioByteChannel extends AbstractNioChannel {
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    private static final String EXPECTED_TYPES =
            " (expected: " + StringUtil.simpleClassName(ByteBuf.class) + ", " +
            StringUtil.simpleClassName(FileRegion.class) + ')';

    //属性flushTask为task任务，主要负责刷新发送缓存链表中的数
    //据 ， 由 于 write 的 数 据 没 有 直 接 写 在 Socket 中 ， 而 是 写 在 了
    //ChannelOutboundBuffer缓存中，所以当调用flush()方法时，会把数
    //据写入Socket中并向网络中发送。因此当缓存中的数据未发送完成
    //时，需要将此任务添加到EventLoop线程中，等待EventLoop线程的再次发送。
    private final Runnable flushTask = new Runnable() {
        @Override
        public void run() {
            // Calling flush0 directly to ensure we not try to flush messages that were added via write(...) in the
            // meantime.
            // 直接调用flush0以确保在此期间不会尝试刷新通过write（…）添加的消息.
            ((AbstractNioUnsafe) unsafe()).flush0();
        }
    };
    private boolean inputClosedSeenErrorOnRead;

    /**
     * Create a new instance
     *
     * @param parent            the parent {@link Channel} by which this instance was created. May be {@code null}
     * @param ch                the underlying {@link SelectableChannel} on which it operates
     */
    protected AbstractNioByteChannel(Channel parent, SelectableChannel ch) {
        super(parent, ch, SelectionKey.OP_READ);
    }

    /**
     * Shutdown the input side of the channel.
     */
    protected abstract ChannelFuture shutdownInput();

    protected boolean isInputShutdown0() {
        return false;
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioByteUnsafe();
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    final boolean shouldBreakReadReady(ChannelConfig config) {
        return isInputShutdown0() && (inputClosedSeenErrorOnRead || !isAllowHalfClosure(config));
    }

    private static boolean isAllowHalfClosure(ChannelConfig config) {
        return config instanceof SocketChannelConfig &&
                ((SocketChannelConfig) config).isAllowHalfClosure();
    }

    protected class NioByteUnsafe extends AbstractNioUnsafe {

        private void closeOnRead(ChannelPipeline pipeline) {
            if (!isInputShutdown0()) {
                if (isAllowHalfClosure(config())) {
                    shutdownInput();
                    pipeline.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
                } else {
                    close(voidPromise());
                }
            } else {
                inputClosedSeenErrorOnRead = true;
                pipeline.fireUserEventTriggered(ChannelInputShutdownReadComplete.INSTANCE);
            }
        }

        private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf, Throwable cause, boolean close,
                RecvByteBufAllocator.Handle allocHandle) {
            if (byteBuf != null) {
                if (byteBuf.isReadable()) {
                    readPending = false;
                    pipeline.fireChannelRead(byteBuf);
                } else {
                    byteBuf.release();
                }
            }
            allocHandle.readComplete();
            pipeline.fireChannelReadComplete();
            pipeline.fireExceptionCaught(cause);

            // If oom will close the read event, release connection.
            // See https://github.com/netty/netty/issues/10434
            if (close || cause instanceof OutOfMemoryError || cause instanceof IOException) {
                closeOnRead(pipeline);
            }
        }

        /**
         * NioByteUnsafe的read()方法的实现思路大概分为以下3步。
         * （1）获取Channel的配置对象、内存分配器ByteBufAllocator，
         * 并计算内存分配器RecvByte BufAllocator.Handle。
         * （2）进入for循环。循环体的作用：使用内存分配器获取数据容
         * 器ByteBuf，调用doReadBytes()方法将数据读取到容器中，如果本次
         * 循环没有读到数据或链路已关闭，则跳出循环。另外，当循环次数达
         * 到属性METADATA的defaultMaxMessagesPerRead次数（默认为16）时，
         * 也会跳出循环。由于TCP传输会产生粘包问题，因此每次读取都会触发
         * channelRead事件，进而调用业务逻辑处理Handler。
         * （3）跳出循环后，表示本次读取已完成。调用allocHandle的
         * readComplete()方法，并记录读取记录，用于下次分配合理内存。
         */
        @Override
        public final void read() {
            //获取 pipeline 通道配置、Channel 管道
            final ChannelConfig config = config();
            //socketChannel 已关闭
            if (shouldBreakReadReady(config)) {
                clearReadPending();
                return;
            }
            //一个Handler的容器，也可以将其理解为一个Handler链。Handler主要处理数据的编/解码和业务逻辑。
            final ChannelPipeline pipeline = pipeline();
            //获取内存分配器，默认为 PooledByteBufAllocator
            final ByteBufAllocator allocator = config.getAllocator();
            final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
            //清空上一次读取的字节数，每次读取时均重新计算
            //字节buf分配器，并计算字节buf分配器Handler
            allocHandle.reset(config);

            ByteBuf byteBuf = null;
            boolean close = false;
            try {
                do {
                    //分配内存
                    // allocator 根据计算器Handle计算此次需要分配多少内存并从内存池中分配
                    byteBuf = allocHandle.allocate(allocator);
                    //获取通道接收缓冲区的数据
                    //设置最后一次分配内存大小加上每次读取的字节数
                    allocHandle.lastBytesRead(doReadBytes(byteBuf));
                    if (allocHandle.lastBytesRead() <= 0) {
                        // nothing was read. release the buffer.
                        //若没有数据可读取，则释放内存
                        byteBuf.release();
                        byteBuf = null;
                        close = allocHandle.lastBytesRead() < 0;
                        if (close) {
                            // There is nothing left to read as we received an EOF.
                            //当读取到-1时，表示Channel 通道已关闭
                            //没必要再继续读
                            readPending = false;
                        }
                        break;
                    }
                    //更新读取消息计数器
                    allocHandle.incMessagesRead(1);
                    readPending = false;
                    //通知通道处理读取数据，触发Channel 管道的fireChannelRead事件
                    pipeline.fireChannelRead(byteBuf);
                    byteBuf = null;
                } while (allocHandle.continueReading());
                //读取操作完毕，记录此次实际读取到的数据的大小，并预测下一次内存分配的大小
                allocHandle.readComplete();
                //触发Channel管道的fireChannelReadComplete事件
                pipeline.fireChannelReadComplete();

                if (close) {
                    //如果Socket通道关闭，则关闭读操作
                    closeOnRead(pipeline);
                }
            } catch (Throwable t) {
                //处理读异常
                handleReadException(pipeline, byteBuf, t, close, allocHandle);
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                //若读操作完毕，则没有配置自动读
                //则从选择Key兴趣集中移除读操作事件
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp();
                }
            }
        }
    }

    /**
     * Write objects to the OS.
     * @param in the collection which contains objects to write.
     * @return The value that should be decremented from the write quantum which starts at
     * {@link ChannelConfig#getWriteSpinCount()}. The typical use cases are as follows:
     * <ul>
     *     <li>0 - if no write was attempted. This is appropriate if an empty {@link ByteBuf} (or other empty content)
     *     is encountered</li>
     *     <li>1 - if a single call to write data was made to the OS</li>
     *     <li>{@link ChannelUtils#WRITE_STATUS_SNDBUF_FULL} - if an attempt to write data was made to the OS, but no
     *     data was accepted</li>
     * </ul>
     * @throws Exception if an I/O exception occurs during write.
     */
    protected final int doWrite0(ChannelOutboundBuffer in) throws Exception {
        Object msg = in.current();
        if (msg == null) {
            // Directly return here so incompleteWrite(...) is not called.
            return 0;
        }
        return doWriteInternal(in, in.current());
    }

    private int doWriteInternal(ChannelOutboundBuffer in, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (!buf.isReadable()) {
                //若可读字节数为0，则从缓存区中移除
                in.remove();
                return 0;
            }
            //实际发送字节数据
            final int localFlushedAmount = doWriteBytes(buf);
            if (localFlushedAmount > 0) {
                //更新字节数据的发送进度
                in.progress(localFlushedAmount);
                if (!buf.isReadable()) {
                    //若可读字节数为0，则从缓存区中移除
                    in.remove();
                }
                return 1;
            }
        } else if (msg instanceof FileRegion) {
            //如果是文件FileRegion消息
            FileRegion region = (FileRegion) msg;
            if (region.transferred() >= region.count()) {
                in.remove();
                return 0;
            }
            //实际写操作
            long localFlushedAmount = doWriteFileRegion(region);
            if (localFlushedAmount > 0) {
                //更新数据的发送进度
                in.progress(localFlushedAmount);
                if (region.transferred() >= region.count()) {
                    //若region已全部发送成功，则从缓存中移除
                    in.remove();
                }
                return 1;
            }
        } else {
            // Should not reach here.
            // 不支持发送其他类型的数据
            throw new Error();
        }
        //当实际发送字节数为0时，返回Integer.MAX_VALUE;
        return WRITE_STATUS_SNDBUF_FULL;
    }

    /**
     * doWrite() 与 doWriteInternal() 方 法 在 AbstractChannel 的
     * flush0()方法中被调用，主要功能是从ChannelOutboundBuffer缓存中
     * 获取待发送的数据，进行循环发送，发送的结果分为以下3种。
     * （1）发送成功，跳出循环直接返回。
     * （2）由于TCP缓存区已满，成功发送的字节数为0，跳出循环，并
     * 将写操作OP_WRITE事件添加到选择Key兴趣事件集中。
     * （3）默认当写了16次数据还未发送完时，把选择Key的OP_WRITE
     * 事件从兴趣事件集中移除，并添加一个flushTask任务，先去执行其他
     * 任务，当检测到此任务时再发送。
     * @param in
     * @throws Exception
     */
    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        //写请求自循环次数，默认为16次
        int writeSpinCount = config().getWriteSpinCount();
        do {
            //获取当前Channel的缓存ChannelOutboundBuffer中的当前待刷新消息
            Object msg = in.current();
            //所有消息都发送了
            if (msg == null) {
                // Wrote all messages.
                //清除Channel选择Key兴趣事件集中的OP_WRITE写操作事件
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                // 直接返回，没必要再添加写任务
                return;
            }
            //发送数据
            writeSpinCount -= doWriteInternal(in, msg);
        } while (writeSpinCount > 0);
        /**
         * 当因缓冲区满了而发送失败时
         * doWriteInternal返回Integer.MAX_VALUE,此时writeSpinCount<0为true
         * 当发送16次还未全部发送完，但每次都写入成功时writeSpinCount为0
         */
        incompleteWrite(writeSpinCount < 0);
    }

    @Override
    protected final Object filterOutboundMessage(Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (buf.isDirect()) {
                return msg;
            }

            return newDirectBuffer(buf);
        }

        if (msg instanceof FileRegion) {
            return msg;
        }

        throw new UnsupportedOperationException(
                "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
    }

    protected final void incompleteWrite(boolean setOpWrite) {
        // Did not write completely.
        if (setOpWrite) {
            //将OP_WRITE写操作事件添加到Channel的选择Key感兴趣事件集中
            setOpWrite();
        } else {
            // It is possible that we have set the write OP, woken up by NIO because the socket is writable, and then
            // use our write quantum. In this case we no longer want to set the write OP because the socket is still
            // writable (as far as we know). We will find out next time we attempt to write if the socket is writable
            // and set the write OP if necessary.
            // 我们可能设置了write OP，因为套接字是可写的，所以被NIO唤醒，然后使用我们的write quantum。
            // 在这种情况下，我们不再需要设置write OP，因为套接字仍然是可写的（据我们所知）。
            // 我们将在下一次尝试写入时发现套接字是否可写，并在必要时设置write OP。

            //清除Channel选择Key兴趣事件集中的OP_WRITE写操作事件
            clearOpWrite();

            // Schedule flush again later so other tasks can be picked up in the meantime
            // 稍后再次安排刷新，以便在此期间可以执行其他任务
            // 将写操作任务添加到EventLoop线程上，以便后续继续发送
            eventLoop().execute(flushTask);
        }
    }

    /**
     * Write a {@link FileRegion}
     *
     * @param region        the {@link FileRegion} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract long doWriteFileRegion(FileRegion region) throws Exception;

    /**
     * Read bytes into the given {@link ByteBuf} and return the amount.
     */
    protected abstract int doReadBytes(ByteBuf buf) throws Exception;

    /**
     * Write bytes form the given {@link ByteBuf} to the underlying {@link java.nio.channels.Channel}.
     * @param buf           the {@link ByteBuf} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract int doWriteBytes(ByteBuf buf) throws Exception;

    protected final void setOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) == 0) {
            key.interestOps(interestOps | SelectionKey.OP_WRITE);
        }
    }

    protected final void clearOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
        }
    }
}
