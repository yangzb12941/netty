/*
 * Copyright 2019 The Netty Project
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
package io.netty.util.internal;

import static io.netty.util.internal.ObjectUtil.checkPositive;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;

/**
 * Common logic for {@link ReferenceCounted} implementations
 *
 * ReferenceCountUpdater 是 AbstractReferenceCountedByteBuf 的
 * 辅助类，用于完成对引用计数值的具体操作.
 *
 * 虽然它的所有功能基本上都与引用计数有关，但与Netty之前的版本相比
 * 有很大的改动，主要是Netty v4.1.38.Final版本采用了乐观锁方式来
 * 修改refCnt，并在修改后进行校验.
 */
public abstract class ReferenceCountUpdater<T extends ReferenceCounted> {
    /*
     * Implementation notes:
     *
     * For the updated int field:
     *   Even => "real" refcount is (refCnt >>> 1)
     *   Odd  => "real" refcount is 0
     *
     * (x & y) appears to be surprisingly expensive relative to (x == y). Thus this class uses
     * a fast-path in some places for most common low values when checking for live (even) refcounts,
     * for example: if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) { ...
     */

    protected ReferenceCountUpdater() { }

    public static long getUnsafeOffset(Class<? extends ReferenceCounted> clz, String fieldName) {
        try {
            if (PlatformDependent.hasUnsafe()) {
                return PlatformDependent.objectFieldOffset(clz.getDeclaredField(fieldName));
            }
        } catch (Throwable ignore) {
            // fall-back
        }
        return -1;
    }

    //获取原子整型字段更新操作类
    protected abstract AtomicIntegerFieldUpdater<T> updater();

    //获取字段偏移量
    protected abstract long unsafeOffset();

    //初始化
    public final int initialValue() {
        return 2;
    }

    public void setInitialValue(T instance) {
        final long offset = unsafeOffset();
        if (offset == -1) {
            updater().set(instance, initialValue());
        } else {
            PlatformDependent.safeConstructPutInt(instance, offset, initialValue());
        }
    }

    private static int realRefCnt(int rawCnt) {
        return rawCnt != 2 && rawCnt != 4 && (rawCnt & 1) != 0 ? 0 : rawCnt >>> 1;
    }

    /**
     * Like {@link #realRefCnt(int)} but throws if refCnt == 0
     * 通过未处理的引用值获取处理后真实的引用次数
     */
    private static int toLiveRealRefCnt(int rawCnt, int decrement) {
        //当rawCnt为偶数时，真实引用值需要右移1位
        if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) {
            return rawCnt >>> 1;
        }
        // odd rawCnt => already deallocated
        // rawCnt为奇数表示已释放，此时会抛出异常
        throw new IllegalReferenceCountException(0, -decrement);
    }

    //非volatile操作引用释放
    private int nonVolatileRawCnt(T instance) {
        // TODO: Once we compile against later versions of Java we can replace the Unsafe usage here by varhandles.
        // TODO:一旦我们针对Java的更高版本进行编译，我们就可以用varhandles替换这里的不安全用法。
        //获取偏移量
        final long offset = unsafeOffset();
        //若偏移量正常，则选择Unsafe的普通get
        //若偏移量获取异常，则选择Unsafe的普通volatile get
        return offset != -1 ? PlatformDependent.getInt(instance, offset) : updater().get(instance);
    }

    public final int refCnt(T instance) {
        return realRefCnt(updater().get(instance));
    }

    public final boolean isLiveNonVolatile(T instance) {
        final long offset = unsafeOffset();
        final int rawCnt = offset != -1 ? PlatformDependent.getInt(instance, offset) : updater().get(instance);

        // The "real" ref count is > 0 if the rawCnt is even.
        return rawCnt == 2 || rawCnt == 4 || rawCnt == 6 || rawCnt == 8 || (rawCnt & 1) == 0;
    }

    /**
     * An unsafe operation that sets the reference count directly
     */
    public final void setRefCnt(T instance, int refCnt) {
        updater().set(instance, refCnt > 0 ? refCnt << 1 : 1); // overflow OK here
    }

    /**
     * Resets the reference count to 1
     */
    public final void resetRefCnt(T instance) {
        updater().set(instance, initialValue());
    }

    public final T retain(T instance) {
        return retain0(instance, 1, 2);
    }

    public final T retain(T instance, int increment) {
        // all changes to the raw count are 2x the "real" change - overflow is OK
        int rawIncrement = checkPositive(increment, "increment") << 1;
        return retain0(instance, increment, rawIncrement);
    }

    // rawIncrement == increment << 1
    //retain()方法在增加了refCnt后，如果出现了溢出，则回滚并抛异常
    //由duplicate()、slice()衍生的ByteBuf与原对象共享底层的
    //Buffer，原对象的引用可能需要增加，引用增加的方法为retain0()。
    private T retain0(T instance, final int increment, final int rawIncrement) {
        //乐观锁，先获取原值，再增加
        int oldRef = updater().getAndAdd(instance, rawIncrement);
        //如果原值不为偶数，则表示ByteBuf已经被释放，无法继续引用
        if (oldRef != 2 && oldRef != 4 && (oldRef & 1) != 0) {
            throw new IllegalReferenceCountException(0, increment);
        }
        // don't pass 0!
        //如果增加后出现了溢出
        if ((oldRef <= 0 && oldRef + rawIncrement >= 0)
                || (oldRef >= 0 && oldRef + rawIncrement < oldRef)) {
            // overflow case
            //则回滚并抛出异常
            updater().getAndAdd(instance, -rawIncrement);
            throw new IllegalReferenceCountException(realRefCnt(oldRef), increment);
        }
        return instance;
    }

    public final boolean release(T instance) {
        /**
         * 先采用普通方法获取refCnt的值，无须采用volatile获取
         * 因为tryFinalRelease0()方法会用CAS更新。
         * 若更新失敗了，则通过 retryRelease0() 方法进行不断循环处理
         * 此处一开始并非调用 retryRelease0() 方法循环尝试来改refCnt的值
         * 这样设计，吞吐量会有所提升
         * 当rawCnt不等于2时，说明还在其他地方引用了此对象
         * 调用nonFinalRelease0()方法，尝试采用CAS使refCnt的值减2
         */
        int rawCnt = nonVolatileRawCnt(instance);
        return rawCnt == 2 ? tryFinalRelease0(instance, 2) || retryRelease0(instance, 1)
                : nonFinalRelease0(instance, 1, rawCnt, toLiveRealRefCnt(rawCnt, 1));
    }

    // release()方法的返回值为true或false，false表示还有引用存在；true表示无引用，
    // 此时会调用ByteBuf的deallocate()方法进行销毁。
    public final boolean release(T instance, int decrement) {
        int rawCnt = nonVolatileRawCnt(instance);
        int realCnt = toLiveRealRefCnt(rawCnt, checkPositive(decrement, "decrement"));
        return decrement == realCnt ? tryFinalRelease0(instance, rawCnt) || retryRelease0(instance, decrement)
                : nonFinalRelease0(instance, decrement, rawCnt, realCnt);
    }

    //采用CAS最终释放，将refCnt设置为1
    private boolean tryFinalRelease0(T instance, int expectRawCnt) {
        return updater().compareAndSet(instance, expectRawCnt, 1); // any odd number will work
    }
    //最后一次释放
    private boolean nonFinalRelease0(T instance, int decrement, int rawCnt, int realCnt) {
        if (decrement < realCnt
                // all changes to the raw count are 2x the "real" change - overflow is OK
                && updater().compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
            return false;
        }
        //若CAS更新失败，则进入 retryRelease0
        return retryRelease0(instance, decrement);
    }

    //非最后一次释放
    private boolean retryRelease0(T instance, int decrement) {
        for (;;) {
            //volatile 获取refCnt的原始值
            //并通过toLiveRealRefCnt()方法将其转化成真正引用次数
            //原始值必须是2的倍数，否则状态为已释放，会抛出异常
            int rawCnt = updater().get(instance), realCnt = toLiveRealRefCnt(rawCnt, decrement);
            //如果引用次数与当前释放次数相等
            if (decrement == realCnt) {
                /**
                 * 尝试最终释放，采用CAS更新refCnt的值为1，若更新成功则返回true
                 * 如果更新失败，说明refCnt的值改变了，则继续进行循环处理
                 */
                if (tryFinalRelease0(instance, rawCnt)) {
                    return true;
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                /**
                 * 引用次数大于当前释放次数
                 * CAS更新refCnt的值，引用原始值-2*当前释放次数
                 * 此处释放为非最后一次释放因此释放成功后会返回false
                 */
                if (updater().compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
            } else {
                throw new IllegalReferenceCountException(realCnt, -decrement);
            }
            Thread.yield(); // this benefits throughput under high contention
        }
    }
}
