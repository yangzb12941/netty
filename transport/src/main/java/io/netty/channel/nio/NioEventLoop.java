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

import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopException;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.IntSupplier;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReflectionUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;

import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SingleThreadEventLoop} implementation which register the {@link Channel}'s to a
 * {@link Selector} and so does the multi-plexing of these in the event loop.
 *
 * 每个NioEventLoop对象都与NIO中的多路复用器Selector一样，要管理成千
 * 上万条链路，所有链路数据的读/写事件都由它来发起。
 *
 * • 开启Selector并初始化。
 * • 把ServerSocketChannel注册到Selector上。
 * • 处理各种I/O事件，如OP_ACCEPT、OP_CONNECT、OP_READ、OP_WRITE事件。
 * • 执行定时调度任务。
 * • 解决JDK空轮询bug。
 * NioEventLoop这些功能的具体实现大部分都是委托其他类来完成
 * 的，其本身只完成数据流的接入工作。这种设计减轻了NioEventLoop
 * 的负担，同时增强了其扩展性。
 *
 *                                             NioEventLoop
 *                      事件处理      开启selector       重构selector       ServerSocketChannel注册在Boss线程上
 *                      Run()      openSelector()    rebuildSelector()    SingleThreadEventLoop()
 *      OP_ACCEPT      OP_READ      OP_WRITE      非I/O任务
 *      NioMessage    NioByte       AbstractNio   SingleThread
 *      Unsafe(read)  Unsafe(read)  Unsafe(read)  EventExecutor(runAllTasks)
 *
 * （1）当 NioEventLoop 中的多路复用器 Selector 轮询到就绪的
 * SelectionKey时，判断Key的readyOps类型是否为OP_ACCEPT，
 * 若是，Key的 attachment 就是NioServerSocketChannel本身，
 * 先获取SelectionKey的attachment对象，再触发此对象的辅助类
 * Unsafe的实现类 NioMessageUnsafe 的read()方法进行处理。
 *
 * （ 2 ） 在 NioMessageUnsafe 的 read() 方 法 中 会 执 行
 * doReadMessages （ 此 处 用 到 了 模 板 设 计 模 式 ） 。 真 正 调 用 的 是
 * AbstractNioMessageChannel 的 子 类 NioServerSocketChannel 的
 * doReadMessages() 方 法 。 此 方 法 最 终 调 用 ServerSocketChannel 的
 * accept()方法，以获取接入的SocketChannel。将accept()方法在
 * AbstractNioChannel的构造方法中设置为非阻塞状态，不管是否有
 * Channel接入，都会立刻返回，并且一次最多默认获取16个，可以通过
 * 设 置 option 参 数 MAX_MESSAGES_PER_READ 来 调 整 。 获 取 到
 * SocketChannel 后 ， 构 建 NioSocketChannel ， 并 把 构 建 好 的
 * NioSocketChannel 对象作为消息msg传送给Handler（此Handler是
 * ServerBootstrapAcceptor ） ， 触 发 Pipeline 管 道 的
 * fireChannelRead()方法，进而触发read事件，最后会调用Handler的
 * channelRead()方法。
 *
 * （3）在 ServerBootstrapAcceptor 的 channelRead() 方法中，把
 * NioSocketChannel 注册到 Worker 线程上，同时绑定 Channel 的 Handler
 * 链。这与5.1节中将 NioServerSocketChannel 注册到Boss线程上类似，
 * 代码流程基本上都一样，只是实现的子类不一样，如后续添加的事件
 * 由OP_ACCEPT换成了OP_READ。通过这一步的分析，读者可以思考，
 * Netty为何要把Channel抽象化？
 */
public final class NioEventLoop extends SingleThreadEventLoop {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioEventLoop.class);

    private static final int CLEANUP_INTERVAL = 256; // XXX Hard-coded value, but won't need customization.

    /*
     * SelectedSelectionKeySet具体做了什么优化呢？
     * 主要是数据结构改变了，用数组替代了HashSet，重写了add()和iterator()方法，使
     * 数 组 的 遍 历 效 率 更 高 。 开 启 优 化 开 关 ， 需 要 将 系 统 属 性
     * io.netty.noKeySetOptimization设置为true。
     */
    private static final boolean DISABLE_KEY_SET_OPTIMIZATION =
            SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);

    private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
    private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;

    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            return selectNow();
        }
    };

    // Workaround for JDK NIO bug.
    //
    // See:
    // - https://bugs.openjdk.java.net/browse/JDK-6427854 for first few dev (unreleased) builds of JDK 7
    // - https://bugs.openjdk.java.net/browse/JDK-6527572 for JDK prior to 5.0u15-rev and 6u10
    // - https://github.com/netty/netty/issues/203
    static {
        if (PlatformDependent.javaVersion() < 7) {
            final String key = "sun.nio.ch.bugLevel";
            final String bugLevel = SystemPropertyUtil.get(key);
            if (bugLevel == null) {
                try {
                    AccessController.doPrivileged(new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            System.setProperty(key, "");
                            return null;
                        }
                    });
                } catch (final SecurityException e) {
                    logger.debug("Unable to get/set System Property: " + key, e);
                }
            }
        }
        //选择器“自动重建阈值”
        int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
        if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
            selectorAutoRebuildThreshold = 0;
        }

        SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;

        if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEY_SET_OPTIMIZATION);
            logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
        }
    }

    /**
     * The NIO {@link Selector}.
     */
    private Selector selector;
    private Selector unwrappedSelector;
    private SelectedSelectionKeySet selectedKeys;

    private final SelectorProvider provider;

    private static final long AWAKE = -1L;
    private static final long NONE = Long.MAX_VALUE;

    // nextWakeupNanos is:
    //    AWAKE            when EL is awake
    //    NONE             when EL is waiting with no wakeup scheduled
    //    other value T    when EL is waiting with wakeup scheduled at time T
    private final AtomicLong nextWakeupNanos = new AtomicLong(AWAKE);

    private final SelectStrategy selectStrategy;

    private volatile int ioRatio = 50;
    private int cancelledKeys;
    private boolean needsToSelectAgain;

    NioEventLoop(NioEventLoopGroup parent, Executor executor, SelectorProvider selectorProvider,
                 SelectStrategy strategy, RejectedExecutionHandler rejectedExecutionHandler,
                 EventLoopTaskQueueFactory taskQueueFactory, EventLoopTaskQueueFactory tailTaskQueueFactory) {
        super(parent, executor, false, newTaskQueue(taskQueueFactory), newTaskQueue(tailTaskQueueFactory),
                rejectedExecutionHandler);
        this.provider = ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
        this.selectStrategy = ObjectUtil.checkNotNull(strategy, "selectStrategy");
        final SelectorTuple selectorTuple = openSelector();
        this.selector = selectorTuple.selector;
        this.unwrappedSelector = selectorTuple.unwrappedSelector;
    }

    private static Queue<Runnable> newTaskQueue(
            EventLoopTaskQueueFactory queueFactory) {
        if (queueFactory == null) {
            return newTaskQueue0(DEFAULT_MAX_PENDING_TASKS);
        }
        return queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS);
    }

    private static final class SelectorTuple {
        final Selector unwrappedSelector;
        final Selector selector;

        SelectorTuple(Selector unwrappedSelector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = unwrappedSelector;
        }

        SelectorTuple(Selector unwrappedSelector, Selector selector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = selector;
        }
    }

    /**
     * 当 初 始 化 NioEventLoop 时 ， 通 过 openSelector() 方 法 开 启
     * Selector。在rebuildSelector()方法中也可调用openSelector()方法。
     *
     * 在NIO中开启Selector（1行代码），只需调用Selector.open()或
     * SelectorProvider 的 openSelector()方法即可。Netty为Selector设置
     * 了 优 化 开 关 ， 如 果 开 启 优 化 开 关 ， 则 通 过 反 射 加 载
     * sun.nio.ch.SelectorImpl 对 象 ， 并 通 过 已 经 优 化 过 的
     * SelectedSelectionKeySet 替 换 sun.nio.ch.SelectorImpl 对 象 中 的
     * selectedKeys 和 publicSelectedKeys 两 个 HashSet 集 合 。
     * 其 中 ，selectedKeys为就绪Key的集合，拥有所有操作事件准备就绪的选择Key；
     * publicSelectedKeys 为 外 部 访 问 就 绪 Key 的 集 合 代 理 ， 由
     * selectedKeys集合包装成不可修改的集合。
     *
     * SelectedSelectionKeySet具体做了什么优化呢？
     * 主要是数据结构改变了，用数组替代了HashSet，重写了add()和iterator()方法，使
     * 数 组 的 遍 历 效 率 更 高 。 开 启 优 化 开 关 ， 需 要 将 系 统 属 性
     * io.netty.noKeySetOptimization设置为true。
     * @return
     */
    private SelectorTuple openSelector() {
        final Selector unwrappedSelector;
        try {
            //创建Selector
            unwrappedSelector = provider.openSelector();
        } catch (IOException e) {
            throw new ChannelException("failed to open a new selector", e);
        }
        //判断是否开启优化开关，默认没有开启直接返回Selector
        if (DISABLE_KEY_SET_OPTIMIZATION) {
            return new SelectorTuple(unwrappedSelector);
        }

        //通过反射创建SelectorImpl对象
        Object maybeSelectorImplClass = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    return Class.forName(
                            "sun.nio.ch.SelectorImpl",
                            false,
                            PlatformDependent.getSystemClassLoader());
                } catch (Throwable cause) {
                    return cause;
                }
            }
        });

        if (!(maybeSelectorImplClass instanceof Class) ||
            // ensure the current selector implementation is what we can instrument.
                //确保当前选择器的实现是我们所能实现的
            !((Class<?>) maybeSelectorImplClass).isAssignableFrom(unwrappedSelector.getClass())) {
            if (maybeSelectorImplClass instanceof Throwable) {
                Throwable t = (Throwable) maybeSelectorImplClass;
                logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, t);
            }
            return new SelectorTuple(unwrappedSelector);
        }

        final Class<?> selectorImplClass = (Class<?>) maybeSelectorImplClass;
        //使用优化后的SelectedSelectionKeySet对象，将JDKsun.nio.ch.SelectorImpl.selectedKeySet替换掉
        final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

        Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    //使用优化后的SelectedSelectionKeySet对象，将JDKsun.nio.ch.SelectorImpl.selectedKeySet替换掉
                    Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                    Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

                    if (PlatformDependent.javaVersion() >= 9 && PlatformDependent.hasUnsafe()) {
                        // Let us try to use sun.misc.Unsafe to replace the SelectionKeySet.
                        // This allows us to also do this in Java9+ without any extra flags.
                        //让我们尝试使用sun.misc.Unsafe替换SelectionKeySet。这使得我们也可以在Java9+中实现这一点，而不需要任何额外的标志。
                        long selectedKeysFieldOffset = PlatformDependent.objectFieldOffset(selectedKeysField);
                        long publicSelectedKeysFieldOffset =
                                PlatformDependent.objectFieldOffset(publicSelectedKeysField);

                        if (selectedKeysFieldOffset != -1 && publicSelectedKeysFieldOffset != -1) {
                            //原SelectorImpl实例selectedKeys和publicSelectedKeys都是Set子类
                            //SelectedSelectionKeySet实现了Set<E>接口，内部使用数组来优化
                            //public abstract class SelectorImpl extends AbstractSelector {
                            //    protected Set<SelectionKey> selectedKeys = new HashSet();
                            //    private Set<SelectionKey> publicSelectedKeys;
                            //......}
                            PlatformDependent.putObject(
                                    unwrappedSelector, selectedKeysFieldOffset, selectedKeySet);
                            PlatformDependent.putObject(
                                    unwrappedSelector, publicSelectedKeysFieldOffset, selectedKeySet);
                            return null;
                        }
                        // We could not retrieve the offset, lets try reflection as last-resort.
                    }
                    //设置为可写
                    Throwable cause = ReflectionUtil.trySetAccessible(selectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }
                    cause = ReflectionUtil.trySetAccessible(publicSelectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }
                    //通过反射得方式把selector的selectedKeys和publicSelectedKeys
                    //使用Netty构造的selectedKeys替换JDK的selectedKeySet
                    selectedKeysField.set(unwrappedSelector, selectedKeySet);
                    publicSelectedKeysField.set(unwrappedSelector, selectedKeySet);
                    return null;
                } catch (NoSuchFieldException e) {
                    return e;
                } catch (IllegalAccessException e) {
                    return e;
                }
            }
        });

        if (maybeException instanceof Exception) {
            selectedKeys = null;
            Exception e = (Exception) maybeException;
            logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, e);
            return new SelectorTuple(unwrappedSelector);
        }
        //把selectedKeySet赋给NioEventLoop的属性，并返回Selector元数据
        selectedKeys = selectedKeySet;
        logger.trace("instrumented a special java.util.Set into: {}", unwrappedSelector);
        return new SelectorTuple(unwrappedSelector,
                                 new SelectedSelectionKeySetSelector(unwrappedSelector, selectedKeySet));
    }

    /**
     * Returns the {@link SelectorProvider} used by this {@link NioEventLoop} to obtain the {@link Selector}.
     */
    public SelectorProvider selectorProvider() {
        return provider;
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    private static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    /**
     * Registers an arbitrary {@link SelectableChannel}, not necessarily created by Netty, to the {@link Selector}
     * of this event loop.  Once the specified {@link SelectableChannel} is registered, the specified {@code task} will
     * be executed by this event loop when the {@link SelectableChannel} is ready.
     */
    public void register(final SelectableChannel ch, final int interestOps, final NioTask<?> task) {
        ObjectUtil.checkNotNull(ch, "ch");
        if (interestOps == 0) {
            throw new IllegalArgumentException("interestOps must be non-zero.");
        }
        if ((interestOps & ~ch.validOps()) != 0) {
            throw new IllegalArgumentException(
                    "invalid interestOps: " + interestOps + "(validOps: " + ch.validOps() + ')');
        }
        ObjectUtil.checkNotNull(task, "task");

        if (isShutdown()) {
            throw new IllegalStateException("event loop shut down");
        }

        if (inEventLoop()) {
            register0(ch, interestOps, task);
        } else {
            try {
                // Offload to the EventLoop as otherwise java.nio.channels.spi.AbstractSelectableChannel.register
                // may block for a long time while trying to obtain an internal lock that may be hold while selecting.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        register0(ch, interestOps, task);
                    }
                }).sync();
            } catch (InterruptedException ignore) {
                // Even if interrupted we did schedule it so just mark the Thread as interrupted.
                Thread.currentThread().interrupt();
            }
        }
    }

    private void register0(SelectableChannel ch, int interestOps, NioTask<?> task) {
        try {
            ch.register(unwrappedSelector, interestOps, task);
        } catch (Exception e) {
            throw new EventLoopException("failed to register a channel", e);
        }
    }

    /**
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    public int getIoRatio() {
        return ioRatio;
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop. Value range from 1-100.
     * The default value is {@code 50}, which means the event loop will try to spend the same amount of time for I/O
     * as for non-I/O tasks. The lower the number the more time can be spent on non-I/O tasks. If value set to
     * {@code 100}, this feature will be disabled and event loop will not attempt to balance I/O and non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)");
        }
        this.ioRatio = ioRatio;
    }

    /**
     * Replaces the current {@link Selector} of this event loop with newly created {@link Selector}s to work
     * around the infamous epoll 100% CPU bug.
     *
     * 用新创建的｛@link Selector｝替换此事件循环的当前｛@link Selector｝，以解决臭名昭著的 epoll 100%CPU错误。
     */
    public void rebuildSelector() {
        if (!inEventLoop()) {
            execute(new Runnable() {
                @Override
                public void run() {
                    rebuildSelector0();
                }
            });
            return;
        }
        rebuildSelector0();
    }

    @Override
    public int registeredChannels() {
        return selector.keys().size() - cancelledKeys;
    }

    private void rebuildSelector0() {
        final Selector oldSelector = selector;
        final SelectorTuple newSelectorTuple;

        if (oldSelector == null) {
            return;
        }

        try {
            //开启新的Selector
            newSelectorTuple = openSelector();
        } catch (Exception e) {
            logger.warn("Failed to create a new Selector.", e);
            return;
        }

        // Register all channels to the new Selector.
        // 将所有通道注册到新选择器。
        int nChannels = 0;
        //遍历旧的Selector上的key
        for (SelectionKey key: oldSelector.keys()) {
            Object a = key.attachment();
            try {
                //判断key是否有效
                if (!key.isValid() || key.channel().keyFor(newSelectorTuple.unwrappedSelector) != null) {
                    continue;
                }
                //在旧的Selector上触发的事件需要取消
                int interestOps = key.interestOps();
                key.cancel();
                //把Channel重新注册到新的Slector上。
                //注册方法register()在两个地方被调用：
                // 一是在端口绑定前，需要把NioServerSocketChannel注册到Boss线程的Selector上；
                // 二是当NioEventLoop监听到有链路接入时，把链路SocketChannel包装成
                //NioSocketChannel ， 并 注 册 到 Woker 线 程 中 。 最 终 调 用
                //NioSocketChannel的辅助对象unsafe的register()方法，unsafe执行
                //父类AbstractUnsafe的register()模板方法。
                SelectionKey newKey = key.channel().register(newSelectorTuple.unwrappedSelector, interestOps, a);
                if (a instanceof AbstractNioChannel) {
                    // Update SelectionKey
                    ((AbstractNioChannel) a).selectionKey = newKey;
                }
                nChannels ++;
            } catch (Exception e) {
                logger.warn("Failed to re-register a Channel to the new Selector.", e);
                if (a instanceof AbstractNioChannel) {
                    AbstractNioChannel ch = (AbstractNioChannel) a;
                    ch.unsafe().close(ch.unsafe().voidPromise());
                } else {
                    @SuppressWarnings("unchecked")
                    NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                    invokeChannelUnregistered(task, key, e);
                }
            }
        }

        selector = newSelectorTuple.selector;
        unwrappedSelector = newSelectorTuple.unwrappedSelector;

        try {
            // time to close the old selector as everything else is registered to the new one
            // 是时候关闭旧选择器了，因为其他所有内容都已注册到新选择器中
            oldSelector.close();
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close the old Selector.", t);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
        }
    }

    /**
     * run()方法主要分三部分：
     *
     * 首先调用select(boolean oldWakenUp)方法轮询就绪的Channel；
     * 然 后 调 用 processSelectedKeys() 方 法 处 理 I/O 事 件 ； 最 后 运 行
     * runAllTasks()方法处理任务队列。
     */
    @Override
    protected void run() {
        int selectCnt = 0;
        for (;;) {
            try {
                int strategy;
                try {
                    strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks());
                    switch (strategy) {
                    case SelectStrategy.CONTINUE:
                        continue;

                    case SelectStrategy.BUSY_WAIT:
                        // fall-through to SELECT since the busy-wait is not supported with NIO
                        // 由于NIO不支持忙等待，因此返回SELECT

                    case SelectStrategy.SELECT:
                        long curDeadlineNanos = nextScheduledTaskDeadlineNanos();
                        if (curDeadlineNanos == -1L) {
                            curDeadlineNanos = NONE; // nothing on the calendar
                        }
                        nextWakeupNanos.set(curDeadlineNanos);
                        try {
                            if (!hasTasks()) {
                                //第一部分，select(boolean oldWakenUp)：主要目的是轮询看看
                                //是否有准备就绪的Channel。在轮询过程中会调用NIO Selector的
                                //selectNow()和select(timeoutMillis)方法。由于对这两个方法的调
                                //用进行了很明显的区分，因此调用这两个方法的条件也有所不同，具
                                //体逻辑如下。
                                strategy = select(curDeadlineNanos);
                            }
                        } finally {
                            // This update is just to help block unnecessary selector wakeups
                            // so use of lazySet is ok (no race condition)
                            // 这个更新只是为了帮助阻止不必要的选择器唤醒，所以lazySet的使用是可以的（没有比赛条件)
                            nextWakeupNanos.lazySet(AWAKE);
                        }
                        // fall through
                    default:
                    }
                } catch (IOException e) {
                    // If we receive an IOException here its because the Selector is messed up. Let's rebuild
                    // the selector and retry. https://github.com/netty/netty/issues/8566
                    rebuildSelector0();
                    selectCnt = 0;
                    handleLoopException(e);
                    continue;
                }
                //检测次数加1，此参数主要用来判断是否为空轮询
                selectCnt++;
                cancelledKeys = 0;
                needsToSelectAgain = false;
                final int ioRatio = this.ioRatio;
                boolean ranTasks;
                if (ioRatio == 100) {
                    try {
                        if (strategy > 0) {
                            processSelectedKeys();
                        }
                    } finally {
                        // Ensure we always run tasks.
                        ranTasks = runAllTasks();
                    }
                } else if (strategy > 0) {
                    final long ioStartTime = System.nanoTime();
                    try {
                        //processSelectedKeys：主要处理第一部分轮询到的就
                        //绪Key，并取出这些SelectionKey及其附件attachment。附件有两种类
                        //型：第一种是AbstractNioChannel，第二种是NioTask。其中，第二种
                        //附件在Netty内部未使用，因此只分析AbstractNioChannel。根据Key
                        //的事件类型触发AbstractNioChannel的unsafe()的不同方法。这些方
                        //法主要是I/O的读/写操作，其具体源码包括附件的注册。
                        processSelectedKeys();
                    } finally {
                        // Ensure we always run tasks.
                        final long ioTime = System.nanoTime() - ioStartTime;
                        ranTasks = runAllTasks(ioTime * (100 - ioRatio) / ioRatio);
                    }
                } else {
                    //runAllTasks 主要目的是执行taskQueue队列和定时
                    //任务队列中的任务，如心跳检测、异步写操作等。首先NioEventLoop
                    //会根据ioRatio（I/O事件与taskQueue运行的时间占比）计算任务执行
                    //时 长 。 由 于 一 个 NioEventLoop 线 程 需 要 管 理 很 多 Channel ， 这 些
                    //Channel的任务可能非常多，若要都执行完，则I/O事件可能得不到及
                    //时处理，因此每执行64个任务后就会检测执行任务的时间是否已用
                    //完，如果执行任务的时间用完了，就不再执行后续的任务了。
                    ranTasks = runAllTasks(0); // This will run the minimum number of tasks
                }

                if (ranTasks || strategy > 0) {
                    //表明触发了JDK selector 获取准备事件空轮询的debug。通过建立新的selector解决这个问题。
                    if (selectCnt > MIN_PREMATURE_SELECTOR_RETURNS && logger.isDebugEnabled()) {
                        logger.debug("Selector.select() returned prematurely {} times in a row for Selector {}.",
                                selectCnt - 1, selector);
                    }
                    selectCnt = 0;
                } else if (unexpectedSelectorWakeup(selectCnt)) { // Unexpected wakeup (unusual case)
                    selectCnt = 0;
                }
            } catch (CancelledKeyException e) {
                // Harmless exception - log anyway
                if (logger.isDebugEnabled()) {
                    logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector {} - JDK bug?",
                            selector, e);
                }
            } catch (Error e) {
                throw e;
            } catch (Throwable t) {
                handleLoopException(t);
            } finally {
                // Always handle shutdown even if the loop processing threw an exception.
                try {
                    if (isShuttingDown()) {
                        closeAll();
                        if (confirmShutdown()) {
                            return;
                        }
                    }
                } catch (Error e) {
                    throw e;
                } catch (Throwable t) {
                    handleLoopException(t);
                }
            }
        }
    }

    // returns true if selectCnt should be reset
    private boolean unexpectedSelectorWakeup(int selectCnt) {
        //线程中断
        if (Thread.interrupted()) {
            // Thread was interrupted so reset selected keys and break so we not run into a busy loop.
            // As this is most likely a bug in the handler of the user or it's client library we will
            // also log it.
            //
            // See https://github.com/netty/netty/issues/2426
            if (logger.isDebugEnabled()) {
                logger.debug("Selector.select() returned prematurely because " +
                        "Thread.currentThread().interrupt() was called. Use " +
                        "NioEventLoop.shutdownGracefully() to shutdown the NioEventLoop.");
            }
            return true;
        }
        if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
                selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
            // The selector returned prematurely many times in a row.
            // Rebuild the selector to work around the problem.
            logger.warn("Selector.select() returned prematurely {} times in a row; rebuilding Selector {}.",
                    selectCnt, selector);
            //重建selector
            rebuildSelector();
            return true;
        }
        return false;
    }

    private static void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    /**
     * 主要处理第一部分轮询到的就绪Key，并取出这些SelectionKey及其附件attachment。
     * 附件有两种类型：第一种是AbstractNioChannel，第二种是NioTask。其中，第二种
     * 附件在Netty内部未使用，因此只分析AbstractNioChannel。根据Key
     * 的事件类型触发AbstractNioChannel的unsafe()的不同方法。这些方
     * 法主要是I/O的读/写操作，其具体源码包括附件的注册。
     */
    private void processSelectedKeys() {
        //判断优化后的selectedKeys是否为空
        if (selectedKeys != null) {
            //优化处理
            processSelectedKeysOptimized();
        } else {
            //原始处理
            processSelectedKeysPlain(selector.selectedKeys());
        }
    }

    @Override
    protected void cleanup() {
        try {
            selector.close();
        } catch (IOException e) {
            logger.warn("Failed to close a selector.", e);
        }
    }

    void cancel(SelectionKey key) {
        key.cancel();
        cancelledKeys ++;
        //当移除次数大于256时
        if (cancelledKeys >= CLEANUP_INTERVAL) {
            cancelledKeys = 0;
            needsToSelectAgain = true;
        }
    }

    private void processSelectedKeysPlain(Set<SelectionKey> selectedKeys) {
        // check if the set is empty and if so just return to not create garbage by
        // creating a new Iterator every time even if there is nothing to process.
        // See https://github.com/netty/netty/issues/597
        if (selectedKeys.isEmpty()) {
            return;
        }

        Iterator<SelectionKey> i = selectedKeys.iterator();
        for (;;) {
            final SelectionKey k = i.next();
            final Object a = k.attachment();
            i.remove();

            if (a instanceof AbstractNioChannel) {
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            if (!i.hasNext()) {
                break;
            }

            if (needsToSelectAgain) {
                selectAgain();
                selectedKeys = selector.selectedKeys();

                // Create the iterator again to avoid ConcurrentModificationException
                if (selectedKeys.isEmpty()) {
                    break;
                } else {
                    i = selectedKeys.iterator();
                }
            }
        }
    }

    private void processSelectedKeysOptimized() {
        for (int i = 0; i < selectedKeys.size; ++i) {
            final SelectionKey k = selectedKeys.keys[i];
            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/2363

            // 将selectedKeys.key[i] 设置null,并快速被JVM回收
            // 无须等到调用其重置再去回收，因为Key的attachment(附件)比较大，很容易造成内存泄漏
            selectedKeys.keys[i] = null;

            final Object a = k.attachment();

            if (a instanceof AbstractNioChannel) {
                //根据Key的就绪事件触发对应的事件方法
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            //判断是否应该再次轮询
            //每当256个channel从Selector上移除时
            //就标记needsToSelectAgain为true

            if (needsToSelectAgain) {
                // 空出数组中的条目，以允许在Channel关闭后将其进行GC处理。请参阅
                // null out entries in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                //清空i+1之后的selectKeys
                selectedKeys.reset(i + 1);
                //重新调用selectNow()方法
                selectAgain();
                // -1+1 = 0 ，从0开始遍历
                i = -1;
            }
        }
    }

    private void processSelectedKey(SelectionKey k, AbstractNioChannel ch) {
        final AbstractNioChannel.NioUnsafe unsafe = ch.unsafe();
        if (!k.isValid()) {
            final EventLoop eventLoop;
            try {
                eventLoop = ch.eventLoop();
            } catch (Throwable ignored) {
                // If the channel implementation throws an exception because there is no event loop, we ignore this
                // because we are only trying to determine if ch is registered to this event loop and thus has authority
                // to close ch.
                return;
            }
            // Only close ch if ch is still registered to this EventLoop. ch could have deregistered from the event loop
            // and thus the SelectionKey could be cancelled as part of the deregistration process, but the channel is
            // still healthy and should not be closed.
            // See https://github.com/netty/netty/issues/5125
            if (eventLoop == this) {
                // close the channel if the key is not valid anymore
                unsafe.close(unsafe.voidPromise());
            }
            return;
        }

        try {
            int readyOps = k.readyOps();
            // We first need to call finishConnect() before try to trigger a read(...) or write(...) as otherwise
            // the NIO JDK channel implementation may throw a NotYetConnectedException.
            if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                // remove OP_CONNECT as otherwise Selector.select(..) will always return without blocking
                // See https://github.com/netty/netty/issues/924
                int ops = k.interestOps();
                ops &= ~SelectionKey.OP_CONNECT;
                k.interestOps(ops);

                unsafe.finishConnect();
            }

            // Process OP_WRITE first as we may be able to write some queued buffers and so free memory.
            if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                // Call forceFlush which will also take care of clear the OP_WRITE once there is nothing left to write
                ch.unsafe().forceFlush();
            }

            // Also check for readOps of 0 to workaround possible JDK bug which may otherwise lead
            // to a spin loop
            if ((readyOps & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0 || readyOps == 0) {
                unsafe.read();
            }
        } catch (CancelledKeyException ignored) {
            unsafe.close(unsafe.voidPromise());
        }
    }

    private static void processSelectedKey(SelectionKey k, NioTask<SelectableChannel> task) {
        int state = 0;
        try {
            task.channelReady(k.channel(), k);
            state = 1;
        } catch (Exception e) {
            k.cancel();
            invokeChannelUnregistered(task, k, e);
            state = 2;
        } finally {
            switch (state) {
            case 0:
                k.cancel();
                invokeChannelUnregistered(task, k, null);
                break;
            case 1:
                if (!k.isValid()) { // Cancelled by channelReady()
                    invokeChannelUnregistered(task, k, null);
                }
                break;
            default:
                 break;
            }
        }
    }

    private void closeAll() {
        selectAgain();
        Set<SelectionKey> keys = selector.keys();
        Collection<AbstractNioChannel> channels = new ArrayList<AbstractNioChannel>(keys.size());
        for (SelectionKey k: keys) {
            Object a = k.attachment();
            if (a instanceof AbstractNioChannel) {
                channels.add((AbstractNioChannel) a);
            } else {
                k.cancel();
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                invokeChannelUnregistered(task, k, null);
            }
        }

        for (AbstractNioChannel ch: channels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    private static void invokeChannelUnregistered(NioTask<SelectableChannel> task, SelectionKey k, Throwable cause) {
        try {
            task.channelUnregistered(k.channel(), cause);
        } catch (Exception e) {
            logger.warn("Unexpected exception while running NioTask.channelUnregistered()", e);
        }
    }


    /**
     * 在默认情况下，其他线程添加任务到taskQueue队列中后，会调用NioEventLoop的wakeup()方法。
     * 这段代码表示在变量wakenUp为false的情况下，会触发Selector的wakeup操作。
     *
     * （1）wakenUp唤醒动作可能在NioEventLoop线程运行的两个阶段
     * 被 触 发 :
     * 第 一 阶 段 有 可 能 在 NioEventLoop 线 程 运 行 于
     * wakenUp.getAndSet(false) 与 selector.select(timeoutMillis) 之
     * 间。此时selector.select能立刻返回，最新任务得到及时执行。
     *
     * 第二阶段可能在selector.select(timeoutMillis)与runAllTasks
     * 之间，此时在runAllTasks执行完本次任务后又添加了新的任务，这些
     * 任务是无法被及时唤醒的。因为此时wakenUp为true，其他的唤醒操作
     * 都会失败，从而导致这部分任务需要等待select超时后才会被执行。
     * 这 对 于 实 时 性 要 求 很 高 的 程 序 来 讲 是 无 法 接 受 的 。 因 此 在
     * selector.select(timeoutMillis) 与 runAllTasks 中 间 加 入 了 if
     * (wakenUp.get())，即若有唤醒动作，则预唤醒一次，以防后续的唤醒操作失败。
     *
     * （ 2 ） 但 由 于 本 书 的 Netty 版 本 ， 在 select() 方 法 里 ， 调 用
     * hasTask()查看任务队列是否有任务。且在进入select()方法前，会把
     * wakenUp设置为false，所以wakenUp.compareAndSet(false, true)会
     * 成功。因此，当添加了新的任务时会调用selectNow()方法，不会等到
     * 超时才执行任务。因此无须在select()方法后再次调用wakeup()方法。
     *
     * （3）wakeup()方法操作耗性能，因此建议在非复杂处理时，尽量不开额外线程。
     *
     * @param inEventLoop
     */
    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            selector.wakeup();
        }
    }

    @Override
    protected boolean beforeScheduledTaskSubmitted(long deadlineNanos) {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get();
    }

    @Override
    protected boolean afterScheduledTaskSubmitted(long deadlineNanos) {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get();
    }

    Selector unwrappedSelector() {
        return unwrappedSelector;
    }

    int selectNow() throws IOException {
        return selector.selectNow();
    }

    /**
     * （ 1 ） 当 定 时 任 务 需 要 触 发 且 之 前 未 轮 询 超时 ， 会 调 用
     * selectNow()方法立刻返回。
     *
     * （2）当定时任务需要触发且之前轮询过（空轮询或阻塞超时轮
     * 询）直接返回时，没必要再调用selectNow()方法。
     *
     * （ 3 ） 若 taskQueue 队 列 中 有 任 务 ， 且 从 EventLoop 线 程 进 入
     * select()方法开始后，一直无其他线程触发唤醒动作，则需要调用
     * selectNow() 方 法 ， 并 立 刻 返 回 。 因 为 在 运 行 select(boolean
     * oldWakenUp) 之 前 ， 若 有 线 程 触 发 了 wakeUp 动 作 ， 则 需 要 保 证
     * tsakQueue队列中的任务得到了及时处理，防止等待timeoutMillis超时后处理。
     *
     * （4）当select(timeoutMillis)阻塞运行时，在以下4种情况下会
     * 正常唤醒线程：其他线程执行了wakeUp唤醒动作、检测到就绪Key、遇
     * 上空轮询、超时自动醒来。唤醒线程后，除了空轮询会继续轮询，其
     * 他正常情况会跳出循环。
     *
     *        到达定时任务触发时间
     *
     *        任务队列有值，且当前预期唤醒标志为false
     *        (为true时表示已经调用过wakeUp,不会阻塞)
     *
     * select
     *                                                                 跳出循环
     *                                       超时自动唤醒    队列是否有值
     *                                                                 轮询次数selectCnt设置为1,进入下一次轮询
     *
     *        阻塞执行select(timeoutMillis)    检测到就绪Key  跳出循环
     *
     *                                       被额外线程唤醒  跳出循环
     *
     *                                                                 轮询次数selectCnt++，进入下一次轮询
     *                                       空轮询次数大于或等于512
     *                                       (返回的selectedKeys=0)
     *                                                                 检测到轮询次数大于或等于512      重新构建Selector跳出循环
     *
     *
     *
     *
     *
     *
     *
     *
     * @param deadlineNanos
     * @return
     * @throws IOException
     */
    private int select(long deadlineNanos) throws IOException {
        if (deadlineNanos == NONE) {
            return selector.select();
        }
        // Timeout will only be 0 if deadline is within 5 microsecs
        // 如果截止时间在5微秒以内，则超时值仅为0
        long timeoutMillis = deadlineToDelayNanos(deadlineNanos + 995000L) / 1000000L;
        //若之前未执行过select，则调用非阻塞的selectNow()方法。
        //selector.select(timeoutMillis)：阻塞检测就绪Channel,除非有就绪Channel 或遇空轮询问题，
        // 或者被其他线程唤醒否则只能等timeoutMillis后自动醒来
        return timeoutMillis <= 0 ? selector.selectNow() : selector.select(timeoutMillis);
    }

    private void selectAgain() {
        needsToSelectAgain = false;
        try {
            selector.selectNow();
        } catch (Throwable t) {
            logger.warn("Failed to update SelectionKeys.", t);
        }
    }
}
