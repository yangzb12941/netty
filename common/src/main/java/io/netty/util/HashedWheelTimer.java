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
package io.netty.util;

import static io.netty.util.internal.ObjectUtil.checkInRange;
import static io.netty.util.internal.ObjectUtil.checkPositive;
import static io.netty.util.internal.ObjectUtil.checkNotNull;

import io.netty.util.concurrent.ImmediateExecutor;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.util.internal.StringUtil.simpleClassName;

/**
 * A {@link Timer} optimized for approximated I/O timeout scheduling.
 *
 * <h3>Tick Duration</h3>
 *
 * As described with 'approximated', this timer does not execute the scheduled
 * {@link TimerTask} on time.  {@link HashedWheelTimer}, on every tick, will
 * check if there are any {@link TimerTask}s behind the schedule and execute
 * them.
 * <p>
 * You can increase or decrease the accuracy of the execution timing by
 * specifying smaller or larger tick duration in the constructor.  In most
 * network applications, I/O timeout does not need to be accurate.  Therefore,
 * the default tick duration is 100 milliseconds and you will not need to try
 * different configurations in most cases.
 *
 * <h3>Ticks per Wheel (Wheel Size)</h3>
 *
 * {@link HashedWheelTimer} maintains a data structure called 'wheel'.
 * To put simply, a wheel is a hash table of {@link TimerTask}s whose hash
 * function is 'dead line of the task'.  The default number of ticks per wheel
 * (i.e. the size of the wheel) is 512.  You could specify a larger value
 * if you are going to schedule a lot of timeouts.
 *
 * <h3>Do not create many instances.</h3>
 *
 * {@link HashedWheelTimer} creates a new thread whenever it is instantiated and
 * started.  Therefore, you should make sure to create only one instance and
 * share it across your application.  One of the common mistakes, that makes
 * your application unresponsive, is to create a new instance for every connection.
 *
 * <h3>Implementation Details</h3>
 *
 * {@link HashedWheelTimer} is based on
 * <a href="https://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="https://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="https://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 *
 * 时间轮的指针走一轮是多久？
 * 时间轮是采用什么容器存储这些task的？
 * 定时任务的运行时间若晚于指针走一轮的终点，则此时此任务该放在哪个刻度？
 *
 * （1）刻度的间隔时间标注为tickDuration，同时将时间轮一轮的
 * 刻度总数标注为wheelLen，两者都是时间轮的属性，可以通过构造方
 * 法 由 使 用 者 传 入 ， 这 样 就 可 以 得 到 时 间 轮 指
 * 针 走 一 轮 的 时 长 = tickDuration*wheelLen。
 *
 * （2）当指针运行到某一刻度时，需要把映射在此刻度上所有的任
 * 务都取出来，而刻度总数在时间轮初始化后就固定了。因此与Map相
 * 似，采用数组标识wheel[]加链表的方式来存储这些task，数组的大小
 * 固定为图7-1中的N，刻度的编号就是wheel[]的下标。
 *
 * （3）每个时间轮启动都会记录其启动时间，同时，每个定时任务
 * 都有其确定的执行时间，用这个执行时间减去时间轮的启动时间，再
 * 除以刻度的持续时长，就能获取这个定时任务需要指针走过多少刻度
 * 才运行，标注为calculated。
 *
 * 时间轮本身记录了当前指针已经走过了多少刻度，标注为tick。
 * 通过 calculated、tick、时间轮刻度总数 wheelLen 来计算定时任务在
 * 哪一刻度执行（此刻度标注为 stopIndex）。需要分为以下几种情况进
 * 行处理。
 *
 * • 当calculated<tick时，说明这项任务已经是旧任务了，可立刻
 * 执行，因此stopIndex=tick。
 *
 * • 当(calculated-tick)<=wheelLen时，stopIndex=(calculatedtick)。
 *
 * • 当 (calculated-tick)>wheelLen 时 ， calculated 肯 定 大 于
 * wheelLen ， 若 wheelLen 是 2 的 整 数 次 幂 ， 则 可 以 运 用 与 运 算
 * stopIndex=calculated&(wheelLen-1)； 若 wheelLen 不 是 2 的 整 数 次
 * 幂，则把它转换成距离最近的2的整数次幂即可。
 *
 * Netty时间轮改造方案制订
 * 每条I/O线程都会构建一个时间轮，当然也可以只构建一个静态的
 * 时间轮，根据链路数量来决定。
 * 当有Channel通道进来时，会触发channelRegistered()方法，在
 * 此方法中，把通道的心跳定时检测任务交给时间轮，再调用其
 * newTimeout()方法把任务添加到时间轮中。
 *
 * 采用时间轮去执行这些定时任务，很明显可以减轻I/O线程的负
 * 担，但这些定时任务同样是放在内存中的，因此设置定时检测时间一
 * 定要注意不宜过长。虽然单机的长连接并发量不会太高，放在内存也
 * 不会有太大的影响。但是若除心跳检测外，用时间轮作为公司的任务
 * 定时调度系统或监控10亿级定时检测任务系统，则此时再放内存，恐
 * 怕再大的内存也会被撑爆，本节通过改造时间轮来解决这个问题？
 *
 * 以监控10亿级定时检测任务系统为例，想要实现这套系统，用传
 * 统轮询方式性能肯定无法满足要求；用时间轮来实现，若将每天10亿
 * 级任务存放到内存中，则肯定会发生内存溢出，但可以通过改造把时
 * 间轮的任务数据存放到其他地方，如数据库Redis、HBase等。但是若
 * 把这些定时检测数据放入Redis中，则此时会引发以下问题。
 *
 * • 这些数据的存放与时间轮刻度如何映射？
 * （1）数据的存放主要考虑获取方便，在获取时，时间轮只需提供
 * 当前刻度编号idx、时间轮唯一标识wheel、时间轮指针走过了多少刻
 * 度tick即可，这些数据代表了时间轮的当前状态。若用HBase来存储，
 * 则可以采用前缀扫描Scan；若用Redis来存储，则可以考虑存放在多个
 * List中，这些List的Key的前缀一致，由node+idx+tick组成。先从
 * Redis中根据前缀获取这些Key，再把对应的定时检测数据捞出来。
 *
 * • 时间轮存储的检测数据有可能在不断地更新，在时间轮指针每
 * 走一刻时，应该如何获取最新的检测数据呢？
 * （2）通过Key可以直接获取在时间轮上映射的任务数据，但这些
 * 数据早已不再是最新数据了，为了防止误报，需要获取最新数据，此
 * 时就需要在这些数据中设置唯一的id与最新数据的id一致，并把这些
 * 原始数据存放在额外的容器中，通过id及时覆盖旧的数据。也可以通
 * 过id链表批量从容器中获取最新数据。
 *
 * • 当时间轮服务宕机或发版重启时，在服务恢复正常后，这些定
 * 时检测数据该如何处理，若像Netty服务那样，直接把这些数据丢弃，
 * 再重写一遍Redis，则可能会遇到数据严重阻塞，还有丢数据的可能，
 * 需要找到其他更好的解决办法。
 * （3）当时间轮所在的服务宕机或重启时，在服务恢复后，只需恢
 * 复时间轮的元数据即可，包括其启动时间、指针目前移动了多少刻
 * 度、时间轮本身唯一标识、每刻度持续时长、指针走一轮的总刻度
 * 值、指针当前所在的刻度编号。根据这些属性重新构建时间轮，无须
 * 做任何数据的回放工作。但是要注意，时间轮指针每走一刻度，就需
 * 要把时间轮当前的状态进行及时更新，时间轮的状态信息也可以存储
 * 到数据库中，如MySQL、Elasticsearch、HBase、Redis。
 *
 */
public class HashedWheelTimer implements Timer {

    static final InternalLogger logger =
            InternalLoggerFactory.getInstance(HashedWheelTimer.class);

    //时间轮实例个数
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    //在服务过程中，时间轮实例个数不能超过64个
    private static final AtomicBoolean WARNED_TOO_MANY_INSTANCES = new AtomicBoolean();
    private static final int INSTANCE_COUNT_LIMIT = 64;
    //刻度持续时常最小，不能小于这个最小值
    private static final long MILLISECOND_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    //内存泄漏检测
    private static final ResourceLeakDetector<HashedWheelTimer> leakDetector = ResourceLeakDetectorFactory.instance()
            .newResourceLeakDetector(HashedWheelTimer.class, 1);

    //原子性更新时间轮工作状态，防止多线程重复操作
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");

    //内存泄漏检测虚引用
    private final ResourceLeakTracker<HashedWheelTimer> leak;
    //用于构建时间轮工作线程的Runnable掌控指针的跳动
    private final Worker worker = new Worker();
    //时间轮工作线程
    private final Thread workerThread;
    //时间轮三种工作状态，分别为初始化、已经启动正在运行、停止
    public static final int WORKER_STATE_INIT = 0;
    public static final int WORKER_STATE_STARTED = 1;
    public static final int WORKER_STATE_SHUTDOWN = 2;
    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    private volatile int workerState; // 0 - init, 1 - started, 2 - shut down
    //没刻度的持续时间
    private final long tickDuration;
    //此数组用于存储映射在时间轮刻度上的任务
    private final HashedWheelBucket[] wheel;
    //时间轮总格子数 -1
    private final int mask;
    //同步计数器，时间轮Worker线程启动后，将同步给调用时间轮的线程
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
    //超时task任务队列，先将任务放入这个队列中
    //再将Worker线程中队列中取出并放入wheel[]的链表中
    private final Queue<HashedWheelTimeout> timeouts = PlatformDependent.newMpscQueue();
    //取消的task任务存放队列，在worker线程中会检测是否有任务需要取消
    //若有，则找到对应的链表，并修改这些取消任务的前后任务的指针
    private final Queue<HashedWheelTimeout> cancelledTimeouts = PlatformDependent.newMpscQueue();
    //目前需要等待执行的任务数
    private final AtomicLong pendingTimeouts = new AtomicLong(0);
    //时间轮最多容纳多少定时检测任务，默认为-1，无限制
    private final long maxPendingTimeouts;

    private final Executor taskExecutor;
    //时间轮启动时间
    private volatile long startTime;

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration the duration between tick
     * @param unit         the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, true);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @param leakDetection {@code true} if leak detection should be enabled always,
     *                      if false it will only be enabled if the worker thread is not
     *                      a daemon thread.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection, -1);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory        a {@link ThreadFactory} that creates a
     *                             background {@link Thread} which is dedicated to
     *                             {@link TimerTask} execution.
     * @param tickDuration         the duration between tick
     * @param unit                 the time unit of the {@code tickDuration}
     * @param ticksPerWheel        the size of the wheel
     * @param leakDetection        {@code true} if leak detection should be enabled always,
     *                             if false it will only be enabled if the worker thread is not
     *                             a daemon thread.
     * @param  maxPendingTimeouts  The maximum number of pending timeouts after which call to
     *                             {@code newTimeout} will result in
     *                             {@link java.util.concurrent.RejectedExecutionException}
     *                             being thrown. No maximum pending timeouts limit is assumed if
     *                             this value is 0 or negative.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
            long maxPendingTimeouts) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection,
                maxPendingTimeouts, ImmediateExecutor.INSTANCE);
    }
    /**
     * Creates a new timer.
     *
     * @param threadFactory        a {@link ThreadFactory} that creates a
     *                             background {@link Thread} which is dedicated to
     *                             {@link TimerTask} execution.线程工厂，用于创建线程
     * @param tickDuration         the duration between tick 刻度持续时长
     * @param unit                 the time unit of the {@code tickDuration} 刻度持续单位
     * @param ticksPerWheel        the size of the wheel 时间轮总刻度
     * @param leakDetection        {@code true} if leak detection should be enabled always,
     *                             if false it will only be enabled if the worker thread is not
     *                             a daemon thread. 是否开启内存泄漏检测
     * @param maxPendingTimeouts   The maximum number of pending timeouts after which call to
     *                             {@code newTimeout} will result in
     *                             {@link java.util.concurrent.RejectedExecutionException}
     *                             being thrown. No maximum pending timeouts limit is assumed if
     *                             this value is 0 or negative. 时间轮可接受最大定时检测任务数
     * @param taskExecutor         The {@link Executor} that is used to execute the submitted {@link TimerTask}s.
     *                             The caller is responsible to shutdown the {@link Executor} once it is not needed
     *                             anymore.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
            long maxPendingTimeouts, Executor taskExecutor) {

        checkNotNull(threadFactory, "threadFactory");
        checkNotNull(unit, "unit");
        checkPositive(tickDuration, "tickDuration");
        checkPositive(ticksPerWheel, "ticksPerWheel");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor");

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        //对时间轮刻度数进行格式化，转化成离ticksPerWheel最近的2的整数次幂
        //并初始化wheel数组
        wheel = createWheel(ticksPerWheel);
        mask = wheel.length - 1;

        // Convert tickDuration to nanos.
        //把刻度持续时长转化为纳秒，这样更加精确
        long duration = unit.toNanos(tickDuration);

        // Prevent overflow.
        //检测持续时长不能太长，但也不能太短
        if (duration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                    "tickDuration: %d (expected: 0 < tickDuration in nanos < %d",
                    tickDuration, Long.MAX_VALUE / wheel.length));
        }

        if (duration < MILLISECOND_NANOS) {
            logger.warn("Configured tickDuration {} smaller than {}, using 1ms.",
                        tickDuration, MILLISECOND_NANOS);
            this.tickDuration = MILLISECOND_NANOS;
        } else {
            this.tickDuration = duration;
        }
        //构建时间轮的Worker线程
        workerThread = threadFactory.newThread(worker);
        //是否需要内存泄漏检测
        leak = leakDetection || !workerThread.isDaemon() ? leakDetector.track(this) : null;
        //最大定时检测任务个数
        this.maxPendingTimeouts = maxPendingTimeouts;
        //时间轮实例个数检测，超过64个会告警
        if (INSTANCE_COUNTER.incrementAndGet() > INSTANCE_COUNT_LIMIT &&
            WARNED_TOO_MANY_INSTANCES.compareAndSet(false, true)) {
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // This object is going to be GCed and it is assumed the ship has sailed to do a proper shutdown. If
            // we have not yet shutdown then we want to make sure we decrement the active instance count.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
            }
        }
    }

    /**
     * 格式化总刻度数
     * 初始化时间轮容器
     * @param ticksPerWheel
     * @return
     */
    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        //ticksPerWheel may not be greater than 2^30
        checkInRange(ticksPerWheel, 1, 1073741824, "ticksPerWheel");
        //格式化
        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i ++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    //找到离ticksPerWheel最近的2的整数次幂
    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        //根据时间轮的状态进行对应的处理
        switch (WORKER_STATE_UPDATER.get(this)) {
            //当时间轮处于初始化状态时，需要启动它
            case WORKER_STATE_INIT:
                //原子性启动
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker.
        // 等待Worker线程初始化成功
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }

        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
                if (leak != null) {
                    boolean closed = leak.close(this);
                    assert closed;
                }
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                workerThread.interrupt();
                try {
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            INSTANCE_COUNTER.decrementAndGet();
            if (leak != null) {
                boolean closed = leak.close(this);
                assert closed;
            }
        }
        return worker.unprocessedTimeouts();
    }

    /**
     * 时间轮HashedWheelTimer 添加定时任务及其启动代码解读如下：
     * @param task
     * @param delay
     * @param unit
     * @return
     */
    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        checkNotNull(task, "task");
        checkNotNull(unit, "unit");
        /**
         * 需等待执行的任务数+1
         * 同时判断是否超过了最大限制
         */
        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts ("
                + pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
                + "timeouts (" + maxPendingTimeouts + ")");
        }
        //若时间轮Worker线程启动，则需要启动
        start();

        // Add the timeout to the timeout queue which will be processed on the next tick.
        // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
        //根据定时任务延时执行时间与时间轮启动时间
        //获取相对时间轮开始后的任务执行延时时间
        //因为时间轮开始启动时间是不会改变的，所以通过这个时间可获取时钟需要跳动的刻度
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Guard against overflow.
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }
        /**
         * 构建定时检测任务，并将其添加到新增定时检测任务队列中，在worker线程中，
         * 会从队列中取出定时检测任务并放到缓存数组wheel中
         */
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * Returns the number of pending timeouts of this {@link Timer}.
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        if (logger.isErrorEnabled()) {
            String resourceType = simpleClassName(HashedWheelTimer.class);
            logger.error("You are creating too many " + resourceType + " instances. " +
                    resourceType + " is a shared resource that must be reused across the JVM, " +
                    "so that only a few instances are created.");
        }
    }

    /**
     * Worker线程是整个时间轮的核心，它拥有一个属性——tick。
     * tick与时间刻度有一定的关联，指针每经过一个刻度后，tick++；
     * tick与mask（时间轮总格子数-1）进行与操作后，就是时间轮指针的
     * 当前刻度序号。在Worker线程中，tick做了以下4件事。
     *
     * • 等待下一刻度运行时间到来。
     * • 从取消任务队列中获取需要取消的任务并处理。
     * • 从任务队列中获取需要执行的定时检测任务，并把它们放入对
     * 应的刻度链表中。
     * • 从当前刻度链表中取出需要执行的定时检测任务，并循环执行
     * 这些定时检测任务的run()方法。
     */
    private final class Worker implements Runnable {
        //调用了时间轮的stop()方法后，将获取其未执行完的任务
        private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();
        //时钟指针跳动次数
        private long tick;

        @Override
        public void run() {
            // Initialize the startTime.
            // 时间轮启动时间
            startTime = System.nanoTime();
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            //Worker线程初始化了，通知调用时间论启动的线程
            startTimeInitialized.countDown();

            do {
                //获取下一个刻度时间轮总体的执行时间
                //当这个时间与时间轮启动时间的和大于当前时间时，线程会睡眠到这个时间点
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    //获取刻度编号，即wheel数组的下标
                    int idx = (int) (tick & mask);
                    //先处理需要取消的任务
                    processCancelledTasks();
                    //获取刻度所在的缓存链表
                    HashedWheelBucket bucket =
                            wheel[idx];
                    //把新增的定时检测任务加入wheel数组的缓存链表中
                    transferTimeoutsToBuckets();
                    //循环执行刻度所在的缓存链表
                    bucket.expireTimeouts(deadline);
                    //执行完后，指针才正式跳动
                    tick++;
                }
                //时间轮状态需要为已启动状态。
            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            //运行到这里说明时间轮停止了，需要把未处理的任务返回
            for (HashedWheelBucket bucket: wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            /**
             * 刚刚加入还未来得及放入时间轮缓存中的超时任务
             * 也需要捞出并放入 unprocessedTimeouts中一起返回
             */
            for (;;) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            //处理需要取消的任务
            processCancelledTasks();
        }

        private void transferTimeoutsToBuckets() {
            // transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
            // adds new timeouts in a loop.
            for (int i = 0; i < 100000; i++) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    // Was cancelled in the meantime.
                    continue;
                }

                long calculated = timeout.deadline / tickDuration;
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                final long ticks = Math.max(calculated, tick); // Ensure we don't schedule for past.
                int stopIndex = (int) (ticks & mask);

                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }

        private void processCancelledTasks() {
            for (;;) {
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                try {
                    timeout.remove();
                } catch (Throwable t) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         */
        private long waitForNextTick() {
            //获取下一刻度时间轮总体的执行时间
            long deadline = tickDuration * (tick + 1);

            for (;;) {
                //当前时间-启动时间
                final long currentTime = System.nanoTime() - startTime;
                //计算需要睡眠的毫秒时间
                //由于再将纳秒转换为毫秒时需要除以 1000000
                //因此需要加上 999999，以防止丢失尾数，任务被提前执行
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;
                /**
                 * 当睡眠时间小于0且等于 Long.MIN_VALUE 时，直接跳过此刻度，否则不睡眠，直接执行任务
                 */
                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                // Check if we run on windows, as if thats the case we will need
                // to round the sleepTime as workaround for a bug that only affect
                // the JVM if it runs on windows.
                //
                // See https://github.com/netty/netty/issues/356
                // Windows 操作系统特殊处理，其Sleep函数是以10ms为单位进行延时的
                // 也就是说，所有小于10且大于0的情况都是10ms,所有大于10且小于20的情况都是20ms
                // 因此这里做了特殊处理，对于小于10ms的，直接不睡眠；对于大于10ms的，去掉尾数
                if (PlatformDependent.isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                    if (sleepTimeMs == 0) {
                        sleepTimeMs = 1;
                    }
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    //当发生异常，发现时间轮状态为 WORKER_STATE_SHUTDOWN 立刻返回
                    if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    private static final class HashedWheelTimeout implements Timeout, Runnable {

        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");

        private final HashedWheelTimer timer;
        private final TimerTask task;
        private final long deadline;

        @SuppressWarnings({"unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
        private volatile int state = ST_INIT;

        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        long remainingRounds;

        // This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
        // As only the workerThread will act on it there is no need for synchronization / volatile.
        HashedWheelTimeout next;
        HashedWheelTimeout prev;

        // The bucket to which the timeout was added
        HashedWheelBucket bucket;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            timer.cancelledTimeouts.add(this);
            return true;
        }

        void remove() {
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                timer.taskExecutor.execute(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown while submit " + TimerTask.class.getSimpleName()
                            + " for execution.", t);
                }
            }
        }

        @Override
        public void run() {
            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192)
               .append(simpleClassName(this))
               .append('(')
               .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining)
                   .append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining)
                   .append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ")
                      .append(task())
                      .append(')')
                      .toString();
        }
    }

    /**
     * Bucket that stores HashedWheelTimeouts. These are stored in a linked-list like datastructure to allow easy
     * removal of HashedWheelTimeouts in the middle. Also the HashedWheelTimeout act as nodes themself and so no
     * extra object creation is needed.
     */
    private static final class HashedWheelBucket {
        // Used for the linked-list datastructure
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * Add {@link HashedWheelTimeout} to this bucket.
         */
        public void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * Expire all {@link HashedWheelTimeout}s for the given {@code deadline}.
         */
        public void expireTimeouts(long deadline) {
            HashedWheelTimeout timeout = head;

            // process all timeouts
            while (timeout != null) {
                HashedWheelTimeout next = timeout.next;
                if (timeout.remainingRounds <= 0) {
                    next = remove(timeout);
                    if (timeout.deadline <= deadline) {
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format(
                                "timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
                    }
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    timeout.remainingRounds --;
                }
                timeout = next;
            }
        }

        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         */
        public void clearTimeouts(Set<Timeout> set) {
            for (;;) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head =  null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // null out prev and next to allow for GC.
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }
}
