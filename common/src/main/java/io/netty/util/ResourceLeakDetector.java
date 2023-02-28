/*
 * Copyright 2013 The Netty Project
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

import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.netty.util.internal.StringUtil.EMPTY_STRING;
import static io.netty.util.internal.StringUtil.NEWLINE;
import static io.netty.util.internal.StringUtil.simpleClassName;

/**
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
 *
 * 内存泄漏检测原理:
 * Netty的内存泄漏检测机制主要是检测 ByteBuf 的内存是否正常释
 * 放。想要实现这种机制，就需要完成以下3步。
 * （1）采集ByteBuf对象。
 * （2）记录ByteBuf的最新调用轨迹信息，方便溯源。
 * （3）检查是否有泄漏，并进行日志输出。
 *
 * 第 一 ， 采 集 入 口 在 内 存 分 配 器 PooledByteBufAllocator 的
 * newDirectBuffer 与 newHeapBuffer 方法中，对返回的ByteBuf对象做一
 * 层 包 装 ， 包 装 类 分 两 种 ： SimpleLeakAwareByteBuf 与
 * AdvancedLeakAwareByteBuf。
 *
 * AdvancedLeakAwareByteBuf 是 SimpleLeakAwareByteBuf的子类，
 * 它 们 的 主 要 作 用 都 是 记 录 ByteBuf 的 调 用 轨 迹 。 区 别 在 于 ，
 * AdvancedLeakAwareByteBuf 记 录 ByteBuf 的 所 有 操 作 ；
 * SimpleLeakAwareByteBuf 只在 ByteBuf 被销毁时告诉内存泄漏检测工具
 * 把正常销毁的对象从检测缓存中移除，方便判断ByteBuf是否泄漏，不记录ByteBuf的操作。
 *
 * 第二，每个 ByteBuf 的最新调用栈信息记录在其弱引用中，这个弱
 * 引用对象与 ByteBuf 都包装在 SimpleLeakAwareByteBuf 类中。弱引用对
 * 象除了记录 ByteBuf 的调用轨迹，还要有关闭检测的功能，因为当
 * ByteBuf 被销毁时需要关闭资源跟踪，并清除对资源对象的引用，防止误报。
 *
 * 第三，在创建弱引用时，需要引用队列的配合。当检测是否有资
 * 源泄漏时，需要遍历引用队列，找到已回收的 ByteBuf 的引用，通过这
 * 些引用判断是否调用了ByteBuf的销毁接口，检测是否有泄漏。
 *
 * Netty先把所有弱引用缓存起来，在ByteBuf被销毁后，再从缓存
 * 中移除对应的弱引用，当遍历到此弱引用时，若发现它已从缓存中移
 * 除，则表示ByteBuf无内存泄漏。此种判断方式有点特别，一般只需在
 * 类中加个原子属性即可。例如，AtomicBoolean，在将其正常销毁后，
 * 把弱引用的原子属性值设为true，当检测判断时，若此原子属性为
 * false，则表示非正常销毁。但Netty未采用这种方式，而是使用缓存
 * 容器来判断是否有泄漏。Netty会把这些弱引用对象强引用起来。由于
 * ByteBuf资源对象被垃圾回收器回收后，若其弱引用对象若没有地方强
 * 关联，则会在下一次被垃圾回收器回收，因此Netty采用全局Set把它
 * 缓存起来，防止弱引用对象在遍历之前被回收。
 *
 * ResourceLeakDetector在整个内存泄漏检测机制中起核心作用。
 * 一种缓冲区资源会创建一个ResourceLeakDetector实例，并监控此缓
 * 冲区类型的池化资源。
 *
 * Netty的内存泄漏检测机制有以下4种检测级别:
 * • DISABLED：表示禁用，不开启检测。
 *
 * • SIMPLE：Netty的默认设置，表示按一定比例采集。若采集的
 * ByteBuf出现泄漏，则打印LEAK:XXX等日志，但没有ByteBuf的任何调
 * 用栈信息输出，因为它使用的包装类是SimpleLeakAwareByteBuf，不会进行记录。
 *
 * • ADVANCED：它的采集与SIMPLE级别的采集一样，但会输出
 * ByteBuf 的 调 用 栈 信 息 ， 因 为 它 使 用 的 包 装 类 是
 * AdvancedLeakAwareByteBuf。
 *
 * • PARANOID：偏执级别，这种级别在ADVANCED的基础上按100%的比例采集。
 * @param <T>
 */
public class ResourceLeakDetector<T> {

    private static final String PROP_LEVEL_OLD = "io.netty.leakDetectionLevel";
    private static final String PROP_LEVEL = "io.netty.leakDetection.level";
    private static final Level DEFAULT_LEVEL = Level.SIMPLE;

    private static final String PROP_TARGET_RECORDS = "io.netty.leakDetection.targetRecords";
    private static final int DEFAULT_TARGET_RECORDS = 4;

    private static final String PROP_SAMPLING_INTERVAL = "io.netty.leakDetection.samplingInterval";
    // There is a minor performance benefit in TLR if this is a power of 2.
    private static final int DEFAULT_SAMPLING_INTERVAL = 128;

    private static final int TARGET_RECORDS;
    static final int SAMPLING_INTERVAL;

    /**
     * Represents the level of resource leak detection.
     * 检测级别
     */
    public enum Level {
        /**
         * Disables resource leak detection.
         * 禁用
         */
        DISABLED,
        /**
         * Enables simplistic sampling resource leak detection which reports there is a leak or not,
         * at the cost of small overhead (default).
         * 默认 ByteBuf在分配时按照一定的比例构建。SimpleLeakAwareByteBuf 不记录调用堆栈信息
         */
        SIMPLE,
        /**
         * Enables advanced sampling resource leak detection which reports where the leaked object was accessed
         * recently at the cost of high overhead.
         * 高级 ByteBuf在分配时按照一定的比例构建。AdvancedLeakAwareByteBuf 记录调用堆栈信息
         *
         */
        ADVANCED,
        /**
         * Enables paranoid resource leak detection which reports where the leaked object was accessed recently,
         * at the cost of the highest possible overhead (for testing purposes only).
         * 偏执 所有ByteBuf在分配时都会构建。AdvancedLeakAwareByteBuf 记录调用堆栈信息
         */
        PARANOID;

        /**
         * Returns level based on string value. Accepts also string that represents ordinal number of enum.
         *
         * @param levelStr - level string : DISABLED, SIMPLE, ADVANCED, PARANOID. Ignores case.
         * @return corresponding level or SIMPLE level in case of no match.
         */
        static Level parseLevel(String levelStr) {
            String trimmedLevelStr = levelStr.trim();
            for (Level l : values()) {
                if (trimmedLevelStr.equalsIgnoreCase(l.name()) || trimmedLevelStr.equals(String.valueOf(l.ordinal()))) {
                    return l;
                }
            }
            return DEFAULT_LEVEL;
        }
    }

    private static Level level;

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ResourceLeakDetector.class);

    static {
        final boolean disabled;
        if (SystemPropertyUtil.get("io.netty.noResourceLeakDetection") != null) {
            disabled = SystemPropertyUtil.getBoolean("io.netty.noResourceLeakDetection", false);
            logger.debug("-Dio.netty.noResourceLeakDetection: {}", disabled);
            logger.warn(
                    "-Dio.netty.noResourceLeakDetection is deprecated. Use '-D{}={}' instead.",
                    PROP_LEVEL, DEFAULT_LEVEL.name().toLowerCase());
        } else {
            disabled = false;
        }

        Level defaultLevel = disabled? Level.DISABLED : DEFAULT_LEVEL;

        // First read old property name
        String levelStr = SystemPropertyUtil.get(PROP_LEVEL_OLD, defaultLevel.name());

        // If new property name is present, use it
        levelStr = SystemPropertyUtil.get(PROP_LEVEL, levelStr);
        Level level = Level.parseLevel(levelStr);

        TARGET_RECORDS = SystemPropertyUtil.getInt(PROP_TARGET_RECORDS, DEFAULT_TARGET_RECORDS);
        SAMPLING_INTERVAL = SystemPropertyUtil.getInt(PROP_SAMPLING_INTERVAL, DEFAULT_SAMPLING_INTERVAL);

        ResourceLeakDetector.level = level;
        if (logger.isDebugEnabled()) {
            logger.debug("-D{}: {}", PROP_LEVEL, level.name().toLowerCase());
            logger.debug("-D{}: {}", PROP_TARGET_RECORDS, TARGET_RECORDS);
        }
    }

    /**
     * @deprecated Use {@link #setLevel(Level)} instead.
     */
    @Deprecated
    public static void setEnabled(boolean enabled) {
        setLevel(enabled? Level.SIMPLE : Level.DISABLED);
    }

    /**
     * Returns {@code true} if resource leak detection is enabled.
     */
    public static boolean isEnabled() {
        return getLevel().ordinal() > Level.DISABLED.ordinal();
    }

    /**
     * Sets the resource leak detection level.
     */
    public static void setLevel(Level level) {
        ResourceLeakDetector.level = ObjectUtil.checkNotNull(level, "level");
    }

    /**
     * Returns the current resource leak detection level.
     */
    public static Level getLevel() {
        return level;
    }

    /** the collection of active resources */
    private final Set<DefaultResourceLeak<?>> allLeaks =
            Collections.newSetFromMap(new ConcurrentHashMap<DefaultResourceLeak<?>, Boolean>());

    private final ReferenceQueue<Object> refQueue = new ReferenceQueue<Object>();
    private final Set<String> reportedLeaks =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final String resourceType;
    private final int samplingInterval;

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(Class<?> resourceType) {
        this(simpleClassName(resourceType));
    }

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(String resourceType) {
        this(resourceType, DEFAULT_SAMPLING_INTERVAL, Long.MAX_VALUE);
    }

    /**
     * @deprecated Use {@link ResourceLeakDetector#ResourceLeakDetector(Class, int)}.
     * <p>
     * This should not be used directly by users of {@link ResourceLeakDetector}.
     * Please use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class)}
     * or {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}
     *
     * @param maxActive This is deprecated and will be ignored.
     */
    @Deprecated
    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        this(resourceType, samplingInterval);
    }

    /**
     * This should not be used directly by users of {@link ResourceLeakDetector}.
     * Please use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class)}
     * or {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}
     */
    @SuppressWarnings("deprecation")
    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval) {
        this(simpleClassName(resourceType), samplingInterval, Long.MAX_VALUE);
    }

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     * <p>
     * @param maxActive This is deprecated and will be ignored.
     */
    @Deprecated
    public ResourceLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        this.resourceType = ObjectUtil.checkNotNull(resourceType, "resourceType");
        this.samplingInterval = samplingInterval;
    }

    /**
     * Creates a new {@link ResourceLeak} which is expected to be closed via {@link ResourceLeak#close()} when the
     * related resource is deallocated.
     *
     * @return the {@link ResourceLeak} or {@code null}
     * @deprecated use {@link #track(Object)}
     */
    @Deprecated
    public final ResourceLeak open(T obj) {
        return track0(obj);
    }

    /**
     * Creates a new {@link ResourceLeakTracker} which is expected to be closed via
     * {@link ResourceLeakTracker#close(Object)} when the related resource is deallocated.
     *
     * ResourceLeakDetector的trace()方法是整套检测机制的入口，提供资
     * 源采集逻辑，运用全局的引用队列和引用缓存Set构建ByteBuf的弱引
     * 用对象，并检测当前监控的资源是否出现了内存泄漏。若出现了内存
     * 泄漏，则输出泄漏报告及内存调用轨迹信息。
     *
     * @return the {@link ResourceLeakTracker} or {@code null}
     */
    @SuppressWarnings("unchecked")
    public final ResourceLeakTracker<T> track(T obj) {
        return track0(obj);
    }

    @SuppressWarnings("unchecked")
    private DefaultResourceLeak track0(T obj) {
        //获取内存泄露检测级别
        Level level = ResourceLeakDetector.level;
        //不检测，也不采集
        if (level == Level.DISABLED) {
            return null;
        }

        /**
         * 当级别比偏执级别低时，获取一个128以内的随机数
         * 若得到的数不为0，则不采集
         * 若为0，这检测是否有泄露，并输出泄露日志，同时创建一个弱引用
         */
        if (level.ordinal() < Level.PARANOID.ordinal()) {
            if ((PlatformDependent.threadLocalRandom().nextInt(samplingInterval)) == 0) {
                reportLeak();
                return new DefaultResourceLeak(obj, refQueue, allLeaks, getInitialHint(resourceType));
            }
            return null;
        }
        //偏执级别都采集
        reportLeak();
        return new DefaultResourceLeak(obj, refQueue, allLeaks, getInitialHint(resourceType));
    }

    private void clearRefQueue() {
        for (;;) {
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }
            ref.dispose();
        }
    }

    /**
     * When the return value is {@code true}, {@link #reportTracedLeak} and {@link #reportUntracedLeak}
     * will be called once a leak is detected, otherwise not.
     *
     * @return {@code true} to enable leak reporting.
     */
    protected boolean needReport() {
        return logger.isErrorEnabled();
    }

    private void reportLeak() {
        if (!needReport()) {
            clearRefQueue();
            return;
        }

        // Detect and report previous leaks.
        // 循环获取引用队列中的弱引用
        for (;;) {
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }

            //检测是否泄露，若未泄露则继续下一次循环
            if (!ref.dispose()) {
                continue;
            }

            //获取buf的调试栈信息
            String records = ref.getReportAndClearRecords();
            //不再输出曾经输出过的泄露记录
            if (reportedLeaks.add(records)) {
                if (records.isEmpty()) {
                    reportUntracedLeak(resourceType);
                } else {
                    //输出内存泄露日志及其调用堆栈
                    reportTracedLeak(resourceType, records);
                }
            }
        }
    }

    /**
     * This method is called when a traced leak is detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportTracedLeak(String resourceType, String records) {
        logger.error(
                "LEAK: {}.release() was not called before it's garbage-collected. " +
                "See https://netty.io/wiki/reference-counted-objects.html for more information.{}",
                resourceType, records);
    }

    /**
     * This method is called when an untraced leak is detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportUntracedLeak(String resourceType) {
        logger.error("LEAK: {}.release() was not called before it's garbage-collected. " +
                "Enable advanced leak reporting to find out where the leak occurred. " +
                "To enable advanced leak reporting, " +
                "specify the JVM option '-D{}={}' or call {}.setLevel() " +
                "See https://netty.io/wiki/reference-counted-objects.html for more information.",
                resourceType, PROP_LEVEL, Level.ADVANCED.name().toLowerCase(), simpleClassName(this));
    }

    /**
     * @deprecated This method will no longer be invoked by {@link ResourceLeakDetector}.
     */
    @Deprecated
    protected void reportInstancesLeak(String resourceType) {
    }

    /**
     * Create a hint object to be attached to an object tracked by this record. Similar to the additional information
     * supplied to {@link ResourceLeakTracker#record(Object)}, will be printed alongside the stack trace of the
     * creation of the resource.
     */
    protected Object getInitialHint(String resourceType) {
        return null;
    }

    /**
     * 实现了 ResourceLeakTracker 接口，主要负责跟踪资源的最近调用轨
     * 迹 ， 同 时 继 承 WeakReference 弱 引 用 。 调 用 轨 迹 的 记 录 被 加 入
     * DefaultResourceLeak 的 Record 链表中，Record链表不会保存所有记
     * 录，因为它的长度有一定的限制。
     * @param <T>
     */
    @SuppressWarnings("deprecation")
    private static final class DefaultResourceLeak<T>
            extends WeakReference<Object> implements ResourceLeakTracker<T>, ResourceLeak {

        @SuppressWarnings("unchecked") // generics and updaters do not mix.
        private static final AtomicReferenceFieldUpdater<DefaultResourceLeak<?>, TraceRecord> headUpdater =
                (AtomicReferenceFieldUpdater)
                        AtomicReferenceFieldUpdater.newUpdater(DefaultResourceLeak.class, TraceRecord.class, "head");

        @SuppressWarnings("unchecked") // generics and updaters do not mix.
        private static final AtomicIntegerFieldUpdater<DefaultResourceLeak<?>> droppedRecordsUpdater =
                (AtomicIntegerFieldUpdater)
                        AtomicIntegerFieldUpdater.newUpdater(DefaultResourceLeak.class, "droppedRecords");

        // 调 用 轨 迹 的 记 录 被 加 入 DefaultResourceLeak 的 Record 链表中，
        // Record链表不会保存所有记录，因为它的长度有一定的限制。
        @SuppressWarnings("unused")
        private volatile TraceRecord head;
        @SuppressWarnings("unused")
        private volatile int droppedRecords;

        private final Set<DefaultResourceLeak<?>> allLeaks;
        private final int trackedHash;

        DefaultResourceLeak(
                Object referent,
                ReferenceQueue<Object> refQueue,
                Set<DefaultResourceLeak<?>> allLeaks,
                Object initialHint) {
            super(referent, refQueue);

            assert referent != null;

            // Store the hash of the tracked object to later assert it in the close(...) method.
            // It's important that we not store a reference to the referent as this would disallow it from
            // be collected via the WeakReference.
            trackedHash = System.identityHashCode(referent);
            allLeaks.add(this);
            // Create a new Record so we always have the creation stacktrace included.
            headUpdater.set(this, initialHint == null ?
                    new TraceRecord(TraceRecord.BOTTOM) : new TraceRecord(TraceRecord.BOTTOM, initialHint));
            this.allLeaks = allLeaks;
        }

        @Override
        public void record() {
            record0(null);
        }

        @Override
        public void record(Object hint) {
            record0(hint);
        }

        /**
         * This method works by exponentially backing off as more records are present in the stack. Each record has a
         * 1 / 2^n chance of dropping the top most record and replacing it with itself. This has a number of convenient
         * properties:
         *
         * <ol>
         * <li>  The current record is always recorded. This is due to the compare and swap dropping the top most
         *       record, rather than the to-be-pushed record.
         * <li>  The very last access will always be recorded. This comes as a property of 1.
         * <li>  It is possible to retain more records than the target, based upon the probability distribution.
         * <li>  It is easy to keep a precise record of the number of elements in the stack, since each element has to
         *     know how tall the stack is.
         * </ol>
         *
         * In this particular implementation, there are also some advantages. A thread local random is used to decide
         * if something should be recorded. This means that if there is a deterministic access pattern, it is now
         * possible to see what other accesses occur, rather than always dropping them. Second, after
         * {@link #TARGET_RECORDS} accesses, backoff occurs. This matches typical access patterns,
         * where there are either a high number of accesses (i.e. a cached buffer), or low (an ephemeral buffer), but
         * not many in between.
         *
         * The use of atomics avoids serializing a high number of accesses, when most of the records will be thrown
         * away. High contention only happens when there are very few existing records, which is only likely when the
         * object isn't shared! If this is a problem, the loop can be aborted and the record dropped, because another
         * thread won the race.
         *
         * 在 ADVANCED 之上的级别的操作中，ByteBuf 的每项操作都涉及线程
         * 调用栈轨迹的记录。那么该如何获取线程栈调用信息呢？
         *
         * 在记录某个点的调用栈信息时，Netty会创建一个Record对象，Record类继承
         * Exception的父类Throwable。因此在创建Record对象时，当前线程的
         * 调用栈信息就会被保存起来。
         */
        //记录调用轨迹
        private void record0(Object hint) {
            // Check TARGET_RECORDS > 0 here to avoid similar check before remove from and add to lastRecords
            // 如果TARGET_RECORDS 大于0，则记录
            if (TARGET_RECORDS > 0) {
                TraceRecord oldHead;
                TraceRecord prevHead;
                TraceRecord newHead;
                boolean dropped;
                do {
                    //判断记录链头是否为空，为空表示已关闭
                    //把之前的链头作为第二个元素赋值给新链表
                    if ((prevHead = oldHead = headUpdater.get(this)) == null) {
                        // already closed.
                        return;
                    }
                    //获取链表长度
                    final int numElements = oldHead.pos + 1;
                    //若链表长度大于或等于最大长度值 TARGET_RECORDS
                    if (numElements >= TARGET_RECORDS) {
                        /**
                         * backOffFactor 是用来计算是否替换的因子
                         * 其最小值为 numElements - TARGET_RECORDS 元素越多，其值越大，最大为30
                         */
                        final int backOffFactor = Math.min(numElements - TARGET_RECORDS, 30);
                        /**
                         * 1/2^backOffFactor 的概率不会执行此if代码块
                         * 代码块：prevHead = oldHead.next
                         * 表示用之前链头元素作为新链表的第二个元素，丢弃原来的链头，同时设置dropped为false
                         */
                        if (dropped = PlatformDependent.threadLocalRandom().nextInt(1 << backOffFactor) != 0) {
                            prevHead = oldHead.next;
                        }
                    } else {
                        dropped = false;
                    }
                    //创建一个新的Record，并将其添加到链表上，作为链表新的头部
                    newHead = hint != null ? new TraceRecord(prevHead, hint) : new TraceRecord(prevHead);
                } while (!headUpdater.compareAndSet(this, oldHead, newHead));
                //若有丢弃，否则更新记录丢弃的数据
                if (dropped) {
                    droppedRecordsUpdater.incrementAndGet(this);
                }
            }
        }

        /**
         * 判断是否泄露
         * @return
         */
        boolean dispose() {
            //清理对资源对象的引用
            clear();
            //若引用缓存中还存在此引用，则说明buf未释放，内存泄漏了
            return allLeaks.remove(this);
        }

        @Override
        public boolean close() {
            if (allLeaks.remove(this)) {
                // Call clear so the reference is not even enqueued.
                clear();
                headUpdater.set(this, null);
                return true;
            }
            return false;
        }

        @Override
        public boolean close(T trackedObject) {
            // Ensure that the object that was tracked is the same as the one that was passed to close(...).
            assert trackedHash == System.identityHashCode(trackedObject);

            try {
                return close();
            } finally {
                // This method will do `synchronized(trackedObject)` and we should be sure this will not cause deadlock.
                // It should not, because somewhere up the callstack should be a (successful) `trackedObject.release`,
                // therefore it is unreasonable that anyone else, anywhere, is holding a lock on the trackedObject.
                // (Unreasonable but possible, unfortunately.)
                reachabilityFence0(trackedObject);
            }
        }

         /**
         * Ensures that the object referenced by the given reference remains
         * <a href="package-summary.html#reachability"><em>strongly reachable</em></a>,
         * regardless of any prior actions of the program that might otherwise cause
         * the object to become unreachable; thus, the referenced object is not
         * reclaimable by garbage collection at least until after the invocation of
         * this method.
         *
         * <p> Recent versions of the JDK have a nasty habit of prematurely deciding objects are unreachable.
         * see: https://stackoverflow.com/questions/26642153/finalize-called-on-strongly-reachable-object-in-java-8
         * The Java 9 method Reference.reachabilityFence offers a solution to this problem.
         *
         * <p> This method is always implemented as a synchronization on {@code ref}, not as
         * {@code Reference.reachabilityFence} for consistency across platforms and to allow building on JDK 6-8.
         * <b>It is the caller's responsibility to ensure that this synchronization will not cause deadlock.</b>
         *
         * @param ref the reference. If {@code null}, this method has no effect.
         * @see java.lang.ref.Reference#reachabilityFence
         */
        private static void reachabilityFence0(Object ref) {
            if (ref != null) {
                synchronized (ref) {
                    // Empty synchronized is ok: https://stackoverflow.com/a/31933260/1151521
                }
            }
        }

        /**
         * 弱引用重写了toString()方法
         * 需注意：若采用IDE工具debug调试代码，则在处理对象时，IDE会自动调用toString()方法
         * @return
         */
        @Override
        public String toString() {
            //获取记录列表的头部
            TraceRecord oldHead = headUpdater.get(this);
            return generateReport(oldHead);
        }

        String getReportAndClearRecords() {
            TraceRecord oldHead = headUpdater.getAndSet(this, null);
            return generateReport(oldHead);
        }

        private String generateReport(TraceRecord oldHead) {
            //若无记录，则返回空字符串
            if (oldHead == null) {
                // Already closed
                return EMPTY_STRING;
            }
            //若记录太长，则会丢弃部分记录，获取丢弃了多少记录
            final int dropped = droppedRecordsUpdater.get(this);
            int duped = 0;
            /**
             * 由于每次在链表新增头部时，其pos=旧的pos+1
             * 因此最新的链表头部的pos就是链表长度
             */
            int present = oldHead.pos + 1;
            // Guess about 2 kilobytes per stack trace
            //设置buf的容量(大概为2KB栈信息*链表长度)，并添加换行符
            StringBuilder buf = new StringBuilder(present * 2048).append(NEWLINE);
            buf.append("Recent access records: ").append(NEWLINE);

            int i = 1;
            Set<String> seen = new HashSet<String>(present);
            for (; oldHead != TraceRecord.BOTTOM; oldHead = oldHead.next) {
                //获取调用栈信息
                String s = oldHead.toString();
                if (seen.add(s)) {
                    //遍历到最初的记录与其他节点的输出有所不同
                    if (oldHead.next == TraceRecord.BOTTOM) {
                        buf.append("Created at:").append(NEWLINE).append(s);
                    } else {
                        buf.append('#').append(i++).append(':').append(NEWLINE).append(s);
                    }
                } else {
                    //出险重复记录
                    duped++;
                }
            }

            //出险重复记录时，加上特殊日志
            if (duped > 0) {
                buf.append(": ")
                        .append(duped)
                        .append(" leak records were discarded because they were duplicates")
                        .append(NEWLINE);
            }

            //若出现记录数超过了TARGET_RECORDS(默认为4)，则输出丢弃了多少记录等额外信息
            //可通过设置
            if (dropped > 0) {
                buf.append(": ")
                   .append(dropped)
                   .append(" leak records were discarded because the leak record count is targeted to ")
                   .append(TARGET_RECORDS)
                   .append(". Use system property ")
                   .append(PROP_TARGET_RECORDS)
                   .append(" to increase the limit.")
                   .append(NEWLINE);
            }

            buf.setLength(buf.length() - NEWLINE.length());
            return buf.toString();
        }
    }

    private static final AtomicReference<String[]> excludedMethods =
            new AtomicReference<String[]>(EmptyArrays.EMPTY_STRINGS);

    public static void addExclusions(Class clz, String ... methodNames) {
        Set<String> nameSet = new HashSet<String>(Arrays.asList(methodNames));
        // Use loop rather than lookup. This avoids knowing the parameters, and doesn't have to handle
        // NoSuchMethodException.
        for (Method method : clz.getDeclaredMethods()) {
            if (nameSet.remove(method.getName()) && nameSet.isEmpty()) {
                break;
            }
        }
        if (!nameSet.isEmpty()) {
            throw new IllegalArgumentException("Can't find '" + nameSet + "' in " + clz.getName());
        }
        String[] oldMethods;
        String[] newMethods;
        do {
            oldMethods = excludedMethods.get();
            newMethods = Arrays.copyOf(oldMethods, oldMethods.length + 2 * methodNames.length);
            for (int i = 0; i < methodNames.length; i++) {
                newMethods[oldMethods.length + i * 2] = clz.getName();
                newMethods[oldMethods.length + i * 2 + 1] = methodNames[i];
            }
        } while (!excludedMethods.compareAndSet(oldMethods, newMethods));
    }

    private static class TraceRecord extends Throwable {
        private static final long serialVersionUID = 6065153674892850720L;

        private static final TraceRecord BOTTOM = new TraceRecord() {
            private static final long serialVersionUID = 7396077602074694571L;

            // Override fillInStackTrace() so we not populate the backtrace via a native call and so leak the
            // Classloader.
            // See https://github.com/netty/netty/pull/10691
            @Override
            public Throwable fillInStackTrace() {
                return this;
            }
        };

        private final String hintString;
        private final TraceRecord next;
        private final int pos;

        TraceRecord(TraceRecord next, Object hint) {
            // This needs to be generated even if toString() is never called as it may change later on.
            hintString = hint instanceof ResourceLeakHint ? ((ResourceLeakHint) hint).toHintString() : hint.toString();
            this.next = next;
            this.pos = next.pos + 1;
        }

        TraceRecord(TraceRecord next) {
           hintString = null;
           this.next = next;
           this.pos = next.pos + 1;
        }

        // Used to terminate the stack
        private TraceRecord() {
            hintString = null;
            next = null;
            pos = -1;
        }

        /**
         * Record的toString()方法，获取Record创建时的调用栈信息
         * @return
         */
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder(2048);
            //先添加提示信息
            if (hintString != null) {
                buf.append("\tHint: ").append(hintString).append(NEWLINE);
            }

            // Append the stack trace.
            //再添加栈信息
            StackTraceElement[] array = getStackTrace();
            // Skip the first three elements.
            // 跳过前面3个栈元素
            // 因为它们是record()方法的栈信息，显示没意义
            out: for (int i = 3; i < array.length; i++) {
                StackTraceElement element = array[i];
                // Strip the noisy stack trace elements.
                // 跳过一些没必要的信息
                String[] exclusions = excludedMethods.get();
                for (int k = 0; k < exclusions.length; k += 2) {
                    // Suppress a warning about out of bounds access
                    // since the length of excludedMethods is always even, see addExclusions()
                    if (exclusions[k].equals(element.getClassName())
                            && exclusions[k + 1].equals(element.getMethodName())) { // lgtm[java/index-out-of-bounds]
                        continue out;
                    }
                }
                //格式化
                buf.append('\t');
                buf.append(element.toString());
                //加上换行
                buf.append(NEWLINE);
            }
            return buf.toString();
        }
    }
}
