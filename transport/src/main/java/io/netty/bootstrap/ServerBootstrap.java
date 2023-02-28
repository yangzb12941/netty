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
package io.netty.bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * {@link Bootstrap} sub-class which allows easy bootstrap of {@link ServerChannel}
 * 第一部分，Netty服务的启动过程及其内部线程处理接入Socket链路的过程；
 * 第二部分，Socket链路数据的读/写。
 *
 * • Selector：多路复用器，是NIO的一个核心组件，在BIO中是没
 * 有的，主要用于监听NIO Channel（通道）的各种事件，并检查一个或
 * 多个通道是否处于可读/写状态。在实现单条线程管理多条链路时，传
 * 统的BIO编程管理多条链路都是通过多线程上下切换来实现的，而NIO
 * 有了Selector后，一个Selector只使用一条线程就可以轮询处理多条链路。
 *
 * • ServerSocketChannel：与普通BIO中的ServerSocket一样，主
 * 要用来监听新加入的TCP连接的通道，而且其启动方式与ServerSocket
 * 的 启 动 方 式 也 非 常 相 似 ， 只 需 要 在 开 启 端 口 监 听 之 前 ， 把
 * ServerSocketChannel注册到Selector上，并设置监听OP_ACCEPT事件即可。
 *
 * • SocketChannel：与普通BIO中的Socket一样，是连接到TCP网络
 * Socket 上 的 Channel 。 它 的 创 建 方 式 有 两 种 ： 一 种 方 式 与
 * ServerSocketChannel的创建方式类似，打开一个SocketChannel，这
 * 种方式主要用于客户端主动连接服务器；另一种方式是当有客户端连
 * 接到ServerSocketChannel上时，会创建一个SocketChannel。
 *
 * Netty服务的启动流程与图5-1中的NIO服务器启动的核心部分没什
 * 么区别，同样需要创建Selector，不过它会开启额外线程去创建；同
 * 样 需 要 打 开 ServerSocketChannel ， 只 是 采 用 的 是
 * NioServerSocketChannel 来 进 行 包 装 ； 同 样 需 要 把
 * ServerSocketChannel注册到Selector上，只是这些事情都是在额外的
 * NioEventLoop线程上执行的，并返回ChannelPromise来异步通知是否注册成功。
 *
 * ServerBootstrap辅助启动过程：
 *    NioEventLoop.openSelector 创建多路复用器
 *    doBind
 *       initAndRegister:
 *          channelFactory.newChannel 此处创建NioServerChannel实例，并打开ServerSocketChannel
 *          init 初始化，设置Channel参数，将ServerBootstrapAcceptor 准备加入ChannelPipeline管道中
 *            setChannelOptions
 *            ChannelPipeline.addLast
 *          NioEventLoop.register
 *            ①、AbstractChannel.register()
 *            ②、AbstractNioChannel.doRegister
 *            ③、NioServerSocketChannel.register
 *            ④、DefaultChannelPipeline.invokeHandlerAddedIfNeeded
 *            ⑤、ChannelPipeline.addLast() 准备处理接入链路
 *       doBind0 绑定端口
 *          AbstractChannel.bind
 *          NioServerSocketChannel.doBind
 *          ServerSocketChannel.bind
 *       AbstractChannel.invokeLater 绑定端口后，最终设置监听OP_ACCEPT事件
 *       -> DefaultChannelPipeline.fireChannelActive
 *             ①、HeadContext.channelActive
 *             ②、HeadContext.readIfIsAutoRead
 *             ③、HeadContext.read
 *             ④、AbstractNioMessageChannel.doBeginRead
 *             ⑤、AbstractNioChannel.doBeginRead
 *
 * Netty服务的启动主要分以下5步。
 * （ 1 ） 创 建 两 个 线 程 组 ， 并 调 用 父 类
 * MultiThreadEventExecutorGroup的构造方法实例化每个线程组的子线
 * 程数组，Boss线程组只设置一条线程，Worker线程组默认线程数为
 * NettyRuntime.availableProcessors()*2。在NioEventLoop线程创建
 * 的同时多路复用器Selector被开启（每条NioEventLoop线程都会开启一个多路复用器）。
 *
 * （ 2 ） 在 AbstractBootstrap 的 initAndRegister 中 ， 通 过
 * ReflectiveChannelFactory.newChannel() 来 反 射 创 建
 * NioServerSocketChannel对象。由于Netty不仅仅只提供TCP NIO服
 * 务，因此此处使用了反射开启ServerSocketChannel通道，并赋值给
 * SelectableChannel的ch属性。
 *
 * （3）初始化NioServerSocketChannel、设置属性attr和参数
 * option，并把 Handler 预添加到 NioServerSocketChannel 的Pipeline管
 * 道中。其中，attr是绑定在每个Channel上的上下文；option一般用来
 * 设置一些Channel的参数；NioServerSocketChannel上的Handler除了
 * 包括用户自定义的，还会加上ServerBootstrapAcceptor。
 *
 * （4）NioEventLoop线程调用AbstractUnsafe.register0()方法，
 * 此方法执行NioServerSocketChannel的doRegister()方法。底层调用
 * ServerSocketChannel 的 register() 方 法 把 Channel 注 册 到 Selector
 * 上，同时带上了附件，此附件为NioServerSocketChannel对象本身。
 * 此处的附件attachment与第（3）步的attr很相似，在后续多路复用器
 * 轮询到事件就绪的SelectionKey时，通过k.attachment获取。当出现
 * 超时或链路未中断或移除时，JVM不会回收此附件。注册成功后，会调
 * 用 DefaultChannelPipeline 的 callHandlerAddedForAllHandlers() 方
 * 法，此方法会执行PendingHandlerCallback回调任务，回调原来在没
 * 有注册之前添加的Handler。此处有点难以理解，在注册之前，先运行
 * 了Pipeline的addLast()方法。
 *
 * （ 5 ） 注 册 成 功 后 会 触 发 ChannelFutureListener 的
 * operationComplete()方法，此方法会带上主线程的ChannelPromise参
 * 数 ， 然 后 调 用 AbstractChannel.bind() 方 法 ； 再 执 行
 * NioServerSocketChannel的doBind()方法绑定端口；当绑定成功后，
 * 会触发active事件，为注册到Selector上的ServerSocketChannel加
 * 上监听OP_ACCEPT事件；最终运行ChannelPromise的safeSetSuccess()
 * 方法唤醒serverBootstrap.bind(port).sync()。
 *
 * Netty服务的启动过程看起来
 * 涉及的类非常多，而且很多地方涉及多线程的交互（有主线程，还有
 * EventLoop线程）。但由于NioServerSocketChannel通道绑定了一条
 * NioEventLoop线程，而这条NioEventLoop线程上开启了Selector多路
 * 复用器，因此这些主要步骤的具体完成工作都会交给NioEventLoop线
 * 程 ， 主 线 程 只 需 完 成 协 调 和 初 始 化 工 作 即 可 。 主 线 程 通 过
 * ChannelPromise获取NioEventLoop线程的执行结果。这里有两个问题需要额外思考。
 *
 * • ServerSocketChannel在注册到Selector上后为何要等到绑定端
 * 口才设置监听OP_ACCEPT事件？提示：跟Netty的事件触发模型有关。
 *
 * • NioServerSocketChannel 的 Handler 管 道
 * DefaultChannelPipeline是如何添加Handler并触发各种事件的？
 */
public class ServerBootstrap extends AbstractBootstrap<ServerBootstrap, ServerChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ServerBootstrap.class);

    // The order in which child ChannelOptions are applied is important they may depend on each other for validation
    // purposes.
    // 子 ChannelOptions 的应用顺序很重要，它们可能相互依赖以进行验证。
    private final Map<ChannelOption<?>, Object> childOptions = new LinkedHashMap<ChannelOption<?>, Object>();
    private final Map<AttributeKey<?>, Object> childAttrs = new ConcurrentHashMap<AttributeKey<?>, Object>();
    private final ServerBootstrapConfig config = new ServerBootstrapConfig(this);
    private volatile EventLoopGroup childGroup;
    private volatile ChannelHandler childHandler;

    public ServerBootstrap() { }

    private ServerBootstrap(ServerBootstrap bootstrap) {
        super(bootstrap);
        childGroup = bootstrap.childGroup;
        childHandler = bootstrap.childHandler;
        synchronized (bootstrap.childOptions) {
            childOptions.putAll(bootstrap.childOptions);
        }
        childAttrs.putAll(bootstrap.childAttrs);
    }

    /**
     * Specify the {@link EventLoopGroup} which is used for the parent (acceptor) and the child (client).
     */
    @Override
    public ServerBootstrap group(EventLoopGroup group) {
        return group(group, group);
    }

    /**
     * Set the {@link EventLoopGroup} for the parent (acceptor) and the child (client). These
     * {@link EventLoopGroup}'s are used to handle all the events and IO for {@link ServerChannel} and
     * {@link Channel}'s.
     */
    public ServerBootstrap group(EventLoopGroup parentGroup, EventLoopGroup childGroup) {
        super.group(parentGroup);
        if (this.childGroup != null) {
            throw new IllegalStateException("childGroup set already");
        }
        this.childGroup = ObjectUtil.checkNotNull(childGroup, "childGroup");
        return this;
    }

    /**
     * Allow to specify a {@link ChannelOption} which is used for the {@link Channel} instances once they get created
     * (after the acceptor accepted the {@link Channel}). Use a value of {@code null} to remove a previous set
     * {@link ChannelOption}.
     */
    public <T> ServerBootstrap childOption(ChannelOption<T> childOption, T value) {
        ObjectUtil.checkNotNull(childOption, "childOption");
        synchronized (childOptions) {
            if (value == null) {
                childOptions.remove(childOption);
            } else {
                childOptions.put(childOption, value);
            }
        }
        return this;
    }

    /**
     * Set the specific {@link AttributeKey} with the given value on every child {@link Channel}. If the value is
     * {@code null} the {@link AttributeKey} is removed
     */
    public <T> ServerBootstrap childAttr(AttributeKey<T> childKey, T value) {
        ObjectUtil.checkNotNull(childKey, "childKey");
        if (value == null) {
            childAttrs.remove(childKey);
        } else {
            childAttrs.put(childKey, value);
        }
        return this;
    }

    /**
     * Set the {@link ChannelHandler} which is used to serve the request for the {@link Channel}'s.
     */
    public ServerBootstrap childHandler(ChannelHandler childHandler) {
        this.childHandler = ObjectUtil.checkNotNull(childHandler, "childHandler");
        return this;
    }

    @Override
    void init(Channel channel) {
        setChannelOptions(channel, newOptionsArray(), logger);
        setAttributes(channel, newAttributesArray());

        ChannelPipeline p = channel.pipeline();

        final EventLoopGroup currentChildGroup = childGroup;
        final ChannelHandler currentChildHandler = childHandler;
        final Entry<ChannelOption<?>, Object>[] currentChildOptions = newOptionsArray(childOptions);
        final Entry<AttributeKey<?>, Object>[] currentChildAttrs = newAttributesArray(childAttrs);

        p.addLast(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(final Channel ch) {
                final ChannelPipeline pipeline = ch.pipeline();
                ChannelHandler handler = config.handler();
                if (handler != null) {
                    pipeline.addLast(handler);
                }

                ch.eventLoop().execute(new Runnable() {
                    @Override
                    public void run() {
                        pipeline.addLast(new ServerBootstrapAcceptor(
                                ch, currentChildGroup, currentChildHandler, currentChildOptions, currentChildAttrs));
                    }
                });
            }
        });
    }

    @Override
    public ServerBootstrap validate() {
        super.validate();
        if (childHandler == null) {
            throw new IllegalStateException("childHandler not set");
        }
        if (childGroup == null) {
            logger.warn("childGroup is not set. Using parentGroup instead.");
            childGroup = config.group();
        }
        return this;
    }

    private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {

        private final EventLoopGroup childGroup;
        private final ChannelHandler childHandler;
        private final Entry<ChannelOption<?>, Object>[] childOptions;
        private final Entry<AttributeKey<?>, Object>[] childAttrs;
        private final Runnable enableAutoReadTask;

        ServerBootstrapAcceptor(
                final Channel channel, EventLoopGroup childGroup, ChannelHandler childHandler,
                Entry<ChannelOption<?>, Object>[] childOptions, Entry<AttributeKey<?>, Object>[] childAttrs) {
            this.childGroup = childGroup;
            this.childHandler = childHandler;
            this.childOptions = childOptions;
            this.childAttrs = childAttrs;

            // Task which is scheduled to re-enable auto-read.
            // It's important to create this Runnable before we try to submit it as otherwise the URLClassLoader may
            // not be able to load the class because of the file limit it already reached.
            //
            // See https://github.com/netty/netty/issues/1328
            enableAutoReadTask = new Runnable() {
                @Override
                public void run() {
                    channel.config().setAutoRead(true);
                }
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            final Channel child = (Channel) msg;

            child.pipeline().addLast(childHandler);

            setChannelOptions(child, childOptions, logger);
            setAttributes(child, childAttrs);

            try {
                childGroup.register(child).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            forceClose(child, future.cause());
                        }
                    }
                });
            } catch (Throwable t) {
                forceClose(child, t);
            }
        }

        private static void forceClose(Channel child, Throwable t) {
            child.unsafe().closeForcibly();
            logger.warn("Failed to register an accepted channel: {}", child, t);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final ChannelConfig config = ctx.channel().config();
            if (config.isAutoRead()) {
                // stop accept new connections for 1 second to allow the channel to recover
                // See https://github.com/netty/netty/issues/1328
                config.setAutoRead(false);
                ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
            }
            // still let the exceptionCaught event flow through the pipeline to give the user
            // a chance to do something with it
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public ServerBootstrap clone() {
        return new ServerBootstrap(this);
    }

    /**
     * Return the configured {@link EventLoopGroup} which will be used for the child channels or {@code null}
     * if non is configured yet.
     *
     * @deprecated Use {@link #config()} instead.
     */
    @Deprecated
    public EventLoopGroup childGroup() {
        return childGroup;
    }

    final ChannelHandler childHandler() {
        return childHandler;
    }

    final Map<ChannelOption<?>, Object> childOptions() {
        synchronized (childOptions) {
            return copiedMap(childOptions);
        }
    }

    final Map<AttributeKey<?>, Object> childAttrs() {
        return copiedMap(childAttrs);
    }

    @Override
    public final ServerBootstrapConfig config() {
        return config;
    }
}
