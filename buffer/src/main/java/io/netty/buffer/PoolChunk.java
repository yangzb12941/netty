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

import io.netty.util.internal.LongCounter;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.PriorityQueue;

/**
 * Description of algorithm for PageRun/PoolSubpage allocation from PoolChunk
 *
 * Notation: The following terms are important to understand the code
 * > page  - a page is the smallest unit of memory chunk that can be allocated
 * > run   - a run is a collection of pages
 * > chunk - a chunk is a collection of runs
 * > in this code chunkSize = maxPages * pageSize
 *
 * To begin we allocate a byte array of size = chunkSize
 * Whenever a ByteBuf of given size needs to be created we search for the first position
 * in the byte array that has enough empty space to accommodate the requested size and
 * return a (long) handle that encodes this offset information, (this memory segment is then
 * marked as reserved so it is always used by exactly one ByteBuf and no more)
 *
 * For simplicity all sizes are normalized according to {@link PoolArena#size2SizeIdx(int)} method.
 * This ensures that when we request for memory segments of size > pageSize the normalizedCapacity
 * equals the next nearest size in {@link SizeClasses}.
 *
 *
 *  A chunk has the following layout:
 *
 *     /-----------------\
 *     | run             |
 *     |                 |
 *     |                 |
 *     |-----------------|
 *     | run             |
 *     |                 |
 *     |-----------------|
 *     | unalloctated    |
 *     | (freed)         |
 *     |                 |
 *     |-----------------|
 *     | subpage         |
 *     |-----------------|
 *     | unallocated     |
 *     | (freed)         |
 *     | ...             |
 *     | ...             |
 *     | ...             |
 *     |                 |
 *     |                 |
 *     |                 |
 *     \-----------------/
 *
 *
 * handle:
 * -------
 * a handle is a long number, the bit layout of a run looks like:
 *
 * oooooooo ooooooos ssssssss ssssssue bbbbbbbb bbbbbbbb bbbbbbbb bbbbbbbb
 *
 * o: runOffset (page offset in the chunk), 15bit
 * s: size (number of pages) of this run, 15bit
 * u: isUsed?, 1bit
 * e: isSubpage?, 1bit
 * b: bitmapIdx of subpage, zero if it's not subpage, 32bit
 *
 * runsAvailMap:
 * ------
 * a map which manages all runs (used and not in used).
 * For each run, the first runOffset and last runOffset are stored in runsAvailMap.
 * key: runOffset
 * value: handle
 *
 * runsAvail:
 * ----------
 * an array of {@link PriorityQueue}.
 * Each queue manages same size of runs.
 * Runs are sorted by offset, so that we always allocate runs with smaller offset.
 *
 *
 * Algorithm:
 * ----------
 *
 *   As we allocate runs, we update values stored in runsAvailMap and runsAvail so that the property is maintained.
 *
 * Initialization -
 *  In the beginning we store the initial run which is the whole chunk.
 *  The initial run:
 *  runOffset = 0
 *  size = chunkSize
 *  isUsed = no
 *  isSubpage = no
 *  bitmapIdx = 0
 *
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) find the first avail run using in runsAvails according to size
 * 2) if pages of run is larger than request pages then split it, and save the tailing run
 *    for later using
 *
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) find a not full subpage according to size.
 *    if it already exists just return, otherwise allocate a new PoolSubpage and call init()
 *    note that this subpage object is added to subpagesPool in the PoolArena when we init() it
 * 2) call subpage.allocate()
 *
 * Algorithm: [free(handle, length, nioBuffer)]
 * ----------
 * 1) if it is a subpage, return the slab back into this subpage
 * 2) if the subpage is not used or it is a run, then start free this run
 * 3) merge continuous avail runs
 * 4) save the merged run
 *
 * Netty引入了jemalloc内存分配算法。
 * Netty内存管理层级结构如图6-1所示，其中右边是内存管理的3个层
 * 级，分别是本地线程缓存、分配区arena、系统内存；左边是内存块区
 * 域，不同大小的内存块对应不同的分配区，总体的分配策略如下。
 *
 * • 为了避免线程间锁的竞争和同步，每个I/O线程都对应一个
 * PoolThreadCache，负责当前线程使用非大内存的快速申请和释放。
 *
 * • 当从PoolThreadCache中获取不到内存时，就从PoolArena的内
 * 存池中分配。当内存使用完并释放时，会将其放到PoolThreadCache
 * 中，方便下次使用。若从PoolArena的内存池中分配不到内存，则从堆
 * 内外内存中申请，申请到的内存叫PoolChunk。当内存块的大小默认为
 * 12MB时，会被放入PoolArea的内存池中，以便重复利用。当申请大内
 * 存时（超过了PoolChunk的默认内存大小12MB），直接在堆外或堆内内
 * 存中创建（不归PoolArea管理），用完后直接回收。本书只介绍Netty
 * 的内存池可重复利用的内存。
 *
 *    ++++++++++++++++++++
 *    +                  +     thread              thread
 *    +  小于512B的对象    +  PoolThreadCache      PoolThreadCache
 *    +                  +
 *    +                  +
 *    + 大于或等于512B且   +
 *    + 小于8KB的对象      +
 *    +                  +    PoolArena           PoolArena
 *    +                  +
 *    + 大于或等于8KB且小于 +
 *    + 或等于16MB的对象   +
 *    +                  +
 *    +------------------+
 *    +                  +
 *    + 大于16MB的对象     +          堆外内存PoolChunk
 *    +                  +
 *    ++++++++++++++++++++
 *
 * （1）Netty在具体分配内存之前，会先获取本次内存分配的大
 * 小。具体的内存分配由PoolArena统一管理，先从线程本地缓存
 * PoolThreadCache中获取，线程本地缓存采用固定长度队列缓存此线程
 * 之前用过的内存。
 * （2）若本地线程无缓存，则判断本次需要分配的内存大小，若小
 * 于512B，则先从PoolArena的tinySubpagePools缓存中获取；若大于或
 * 等于512B且小于8KB，则先从smallSubpagePools缓存中获取，上述两
 * 种情况缓存的对象都是PoolChunk分配的PoolSubpage；若大于或等于
 * 8KB或在SubpagePools缓存中分配失败，则从PoolChunkList中查找可
 * 分配的PoolChunk。
 * （3）若PoolChunkList分配失败，则创建新的PoolChunk，由
 * PoolChunk完成具体的分配工作，最终分配成功后，加入对应的
 * PoolChunkList中。若分配的是小于8KB的内存，则需要把从PoolChunk
 * 中分配的PoolSubpage加入PoolArena的SubpagePools中。
 *
 * PoolArena(内存分配器)
 *   PoolThreadCache(线程本地缓存)
 *     MemoryRegionCache
 *       Queue<Entry<T>> queue
 *   PoolSubPage[] tinySubpagePools(小于512B内存分配缓冲区)
 *   PoolSubpage[] smallSubpagePools(大于或等于512B且小于8KB内存分配缓冲区)
 *   PoolChunkList q050
 *     PollChunk
 *        PoolSubpage
 */
//可以把Chunk看作一块大的内存，这块内存被分成了很多小块的内
//存，Netty在使用内存时，会用到其中一块或多块小内存。
// PoolChunk内部维护了一棵平衡二叉树，默认由2048个page组成，
//一个page默认为8KB，整个Chunk默认为16MB
final class PoolChunk<T> implements PoolChunkMetric {
    private static final int SIZE_BIT_LENGTH = 15;
    private static final int INUSED_BIT_LENGTH = 1;
    private static final int SUBPAGE_BIT_LENGTH = 1;
    private static final int BITMAP_IDX_BIT_LENGTH = 32;

    static final int IS_SUBPAGE_SHIFT = BITMAP_IDX_BIT_LENGTH;
    static final int IS_USED_SHIFT = SUBPAGE_BIT_LENGTH + IS_SUBPAGE_SHIFT;
    static final int SIZE_SHIFT = INUSED_BIT_LENGTH + IS_USED_SHIFT;
    static final int RUN_OFFSET_SHIFT = SIZE_BIT_LENGTH + SIZE_SHIFT;

    final PoolArena<T> arena;
    final Object base;
    final T memory;
    final boolean unpooled;

    /**
     * store the first page and last page of each avail run
     */
    private final LongLongHashMap runsAvailMap;

    /**
     * manage all avail runs
     */
    private final LongPriorityQueue[] runsAvail;

    /**
     * manage all subpages in this chunk
     * 管理此区块中的所有子页面
     *
     * 它的数组长度与 PoolChunk 的 page
     * 节点数一致，都是2048，由于它只分配小于8KB的数据，且 PoolChunk
     * 的每个 page 节点只能分配一种 PoolSubpage ，因此 subpages 的下标与
     * PoolChunk的page节点的偏移位置是一一对应的。
     */
    private final PoolSubpage<T>[] subpages;

    /**
     * Accounting of pinned memory – memory that is currently in use by ByteBuf instances.
     */
    private final LongCounter pinnedBytes = PlatformDependent.newLongCounter();

    private final int pageSize;
    private final int pageShifts;
    private final int chunkSize;

    // Use as cache for ByteBuffer created from the memory. These are just duplicates and so are only a container
    // around the memory itself. These are often needed for operations within the Pooled*ByteBuf and so
    // may produce extra GC, which can be greatly reduced by caching the duplicates.
    //
    // This may be null if the PoolChunk is unpooled as pooling the ByteBuffer instances does not make any sense here.
    private final Deque<ByteBuffer> cachedNioBuffers;

    int freeBytes;

    PoolChunkList<T> parent;
    PoolChunk<T> prev;
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    // 测试在争用情况下添加填充是否有帮助
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    @SuppressWarnings("unchecked")
    PoolChunk(PoolArena<T> arena, Object base, T memory, int pageSize, int pageShifts, int chunkSize, int maxPageIdx) {
        unpooled = false;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        freeBytes = chunkSize;

        runsAvail = newRunsAvailqueueArray(maxPageIdx);
        runsAvailMap = new LongLongHashMap(-1);
        subpages = new PoolSubpage[chunkSize >> pageShifts];

        //insert initial run, offset = 0, pages = chunkSize / pageSize
        int pages = chunkSize >> pageShifts;
        long initHandle = (long) pages << SIZE_SHIFT;
        insertAvailRun(0, pages, initHandle);

        cachedNioBuffers = new ArrayDeque<ByteBuffer>(8);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, Object base, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        pageSize = 0;
        pageShifts = 0;
        runsAvailMap = null;
        runsAvail = null;
        subpages = null;
        chunkSize = size;
        cachedNioBuffers = null;
    }

    private static LongPriorityQueue[] newRunsAvailqueueArray(int size) {
        LongPriorityQueue[] queueArray = new LongPriorityQueue[size];
        for (int i = 0; i < queueArray.length; i++) {
            queueArray[i] = new LongPriorityQueue();
        }
        return queueArray;
    }

    private void insertAvailRun(int runOffset, int pages, long handle) {
        int pageIdxFloor = arena.pages2pageIdxFloor(pages);
        LongPriorityQueue queue = runsAvail[pageIdxFloor];
        queue.offer(handle);

        //insert first page of run
        insertAvailRun0(runOffset, handle);
        if (pages > 1) {
            //insert last page of run
            insertAvailRun0(lastPage(runOffset, pages), handle);
        }
    }

    private void insertAvailRun0(int runOffset, long handle) {
        long pre = runsAvailMap.put(runOffset, handle);
        assert pre == -1;
    }

    private void removeAvailRun(long handle) {
        int pageIdxFloor = arena.pages2pageIdxFloor(runPages(handle));
        LongPriorityQueue queue = runsAvail[pageIdxFloor];
        removeAvailRun(queue, handle);
    }

    private void removeAvailRun(LongPriorityQueue queue, long handle) {
        queue.remove(handle);

        int runOffset = runOffset(handle);
        int pages = runPages(handle);
        //remove first page of run
        runsAvailMap.remove(runOffset);
        if (pages > 1) {
            //remove last page of run
            runsAvailMap.remove(lastPage(runOffset, pages));
        }
    }

    private static int lastPage(int runOffset, int pages) {
        return runOffset + pages - 1;
    }

    private long getAvailRunByOffset(int runOffset) {
        return runsAvailMap.get(runOffset);
    }

    @Override
    public int usage() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
        final long handle;
        if (sizeIdx <= arena.smallMaxSizeIdx) {
            // small
            handle = allocateSubpage(sizeIdx);
            if (handle < 0) {
                return false;
            }
            assert isSubpage(handle);
        } else {
            // normal
            // runSize must be multiple of pageSize
            int runSize = arena.sizeIdx2size(sizeIdx);
            handle = allocateRun(runSize);
            if (handle < 0) {
                return false;
            }
            assert !isSubpage(handle);
        }

        ByteBuffer nioBuffer = cachedNioBuffers != null? cachedNioBuffers.pollLast() : null;
        initBuf(buf, nioBuffer, handle, reqCapacity, cache);
        return true;
    }

    /**
     *
     * @param runSize 申请的内存在PoolChunck二叉树中的高度值，若内存为8KB，则runSize为11
     * @return
     */
    private long allocateRun(int runSize) {
        int pages = runSize >> pageShifts;
        int pageIdx = arena.pages2pageIdx(pages);

        synchronized (runsAvail) {
            //find first queue which has at least one big enough run
            //找到至少有一个足够大的运行的第一个队列
            int queueIdx = runFirstBestFit(pageIdx);
            if (queueIdx == -1) {
                return -1;
            }

            //get run with min offset in this queue
            //使用此队列中的最小偏移量运行
            LongPriorityQueue queue = runsAvail[queueIdx];
            long handle = queue.poll();

            assert handle != LongPriorityQueue.NO_VALUE && !isUsed(handle) : "invalid handle: " + handle;

            removeAvailRun(queue, handle);

            if (handle != -1) {
                handle = splitLargeRun(handle, pages);
            }

            int pinnedSize = runSize(pageShifts, handle);
            freeBytes -= pinnedSize;
            return handle;
        }
    }

    private int calculateRunSize(int sizeIdx) {
        int maxElements = 1 << pageShifts - SizeClasses.LOG2_QUANTUM;
        int runSize = 0;
        int nElements;

        final int elemSize = arena.sizeIdx2size(sizeIdx);

        //find lowest common multiple of pageSize and elemSize
        do {
            runSize += pageSize;
            nElements = runSize / elemSize;
        } while (nElements < maxElements && runSize != nElements * elemSize);

        while (nElements > maxElements) {
            runSize -= pageSize;
            nElements = runSize / elemSize;
        }

        assert nElements > 0;
        assert runSize <= chunkSize;
        assert runSize >= elemSize;

        return runSize;
    }

    private int runFirstBestFit(int pageIdx) {
        if (freeBytes == chunkSize) {
            return arena.nPSizes - 1;
        }
        for (int i = pageIdx; i < arena.nPSizes; i++) {
            LongPriorityQueue queue = runsAvail[i];
            if (queue != null && !queue.isEmpty()) {
                return i;
            }
        }
        return -1;
    }

    private long splitLargeRun(long handle, int needPages) {
        assert needPages > 0;

        int totalPages = runPages(handle);
        assert needPages <= totalPages;

        int remPages = totalPages - needPages;

        if (remPages > 0) {
            int runOffset = runOffset(handle);

            // keep track of trailing unused pages for later use
            int availOffset = runOffset + needPages;
            long availRun = toRunHandle(availOffset, remPages, 0);
            insertAvailRun(availOffset, remPages, availRun);

            // not avail
            return toRunHandle(runOffset, needPages, 1);
        }

        //mark it as used
        handle |= 1L << IS_USED_SHIFT;
        return handle;
    }

    /**
     * Create / initialize a new PoolSubpage of normCapacity. Any PoolSubpage created / initialized here is added to
     * subpage pool in the PoolArena that owns this PoolChunk
     * 当申请内存小于8KB时，此方法被调用
     * @param sizeIdx sizeIdx of normalized size
     *
     * @return index in memoryMap
     */
    private long allocateSubpage(int sizeIdx) {
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // This is need as we may add it back and so alter the linked-list structure.
        // 获取PoolArena拥有的PoolSubPage池的头部并在其上进行同步。这是需要的，因为我们可能会将其添加回去，从而更改链接列表结构。
        //通过优化后的内存容量找到Arena的两个subpages 缓存池中一个的对应空间head指针
        PoolSubpage<T> head = arena.findSubpagePoolHead(sizeIdx);
        //由于分配前需要把 PoolSubpage 加入到缓存池中，以便下回直接从Arena的缓存池中获取
        synchronized (head) {
            //allocate a new run
            // 获取一个可用的节点
            int runSize = calculateRunSize(sizeIdx);
            //runSize must be multiples of pageSize
            long runHandle = allocateRun(runSize);
            if (runHandle < 0) {
                return -1;
            }

            int runOffset = runOffset(runHandle);
            assert subpages[runOffset] == null;
            int elemSize = arena.sizeIdx2size(sizeIdx);
            // 初始化一个 PoolSubpage，调用PoolSubpage的addToPool()方法把subpage追加到head的后面。
            PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
                               runSize(pageShifts, runHandle), elemSize);

            subpages[runOffset] = subpage;
            //PoolSubpage的内存分配
            return subpage.allocate();
        }
    }

    /**
     * Free a subpage or a run of pages When a subpage is freed from PoolSubpage, it might be added back to subpage pool
     * of the owning PoolArena. If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize,
     * we can completely free the owning Page so it is available for subsequent allocations
     *内存的释放相对其分配来说要简单很多，下面主要剖析PoolChunk
     * 和PoolSubpage的释放。当内存释放时，同样先根据handle指针找到内
     * 存在PoolChunk和PoolSubpage中的相对偏移量，具体释放步骤如下。
     * （1）若在PoolSubpage上的偏移量大于0，则交给PoolSubpage去
     * 释放，这与PoolSubpage内存申请有些相似，根据PoolSubpage内存分
     * 配段的偏移位bitmapIdx找到long[]数组bitmap的索引q，将bitmap[q]
     * 的 具 体 内 存 占 用 位 r 置 为 0 （ 表 示 释 放 ） 。 同 时 调 整 Arena 中 的
     * PoolSubpage缓存池，若PoolSubpage已全部释放了，且池中除了它还
     * 有其他节点，则从池中移除；若由于之前PoolSubpage的内存段全部分
     * 配完并从池中移除过，则在其当前可用内存段numAvail等于-1且
     * PoolSubpage 释 放 后 ， 对 可 用 内 存 段 进 行 “++” 运 算 ， 从 而 使
     * numAvail++ 等 于 0 ， 此 时 会 把 释 放 的 PoolSubpage 追 加 到 Arena 的
     * PoolSubpage缓存池中，方便下次直接从缓冲池中获取。
     * （2）若在PoolSubpage上的偏移量等于0，或者PoolSubpage释放
     * 完 后 返 回 false （ PoolSubpage 已 全 部 释 放 完 ， 同 时 从 Arena 的
     * PoolSubpage缓存池中移除了），则只需更新PoolChunk二叉树对应节
     * 点的高度值，并更新其所有父节点的高度值及可用字节数即可。
     * @param handle handle to free
     */
    void free(long handle, int normCapacity, ByteBuffer nioBuffer) {
        int runSize = runSize(pageShifts, handle);
        if (isSubpage(handle)) {//先释放subpage
            int sizeIdx = arena.size2SizeIdx(normCapacity);
            //与分配时一样，先去Arena池中找到subpage对应的head指针
            PoolSubpage<T> head = arena.findSubpagePoolHead(sizeIdx);

            int sIdx = runOffset(handle);
            PoolSubpage<T> subpage = subpages[sIdx];
            assert subpage != null && subpage.doNotDestroy;

            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            // 获取PoolArena拥有的PoolSubPage池的头部并在其上进行同步。这是需要的，因为我们可能会将其添加回去，从而更改链接列表结构。
            synchronized (head) {
                //获取32位bitmap交给PoolSubpage释放。释放后返回true，不再继续释放
                if (subpage.free(head, bitmapIdx(handle))) {
                    //the subpage is still used, do not free it
                    return;
                }
                assert !subpage.doNotDestroy;
                // Null out slot in the array as it was freed and we should not use it anymore.
                subpages[sIdx] = null;
            }
        }

        //start free run
        synchronized (runsAvail) {
            // collapse continuous runs, successfully collapsed runs
            // will be removed from runsAvail and runsAvailMap
            long finalRun = collapseRuns(handle);

            //set run as not used
            finalRun &= ~(1L << IS_USED_SHIFT);
            //if it is a subpage, set it to run
            finalRun &= ~(1L << IS_SUBPAGE_SHIFT);

            insertAvailRun(runOffset(finalRun), runPages(finalRun), finalRun);
            //释放的字节数调整
            freeBytes += runSize;
        }

        if (nioBuffer != null && cachedNioBuffers != null &&
            cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
            //把nioBuffer放入缓存队列中，以便下次直接使用
            cachedNioBuffers.offer(nioBuffer);
        }
    }

    private long collapseRuns(long handle) {
        return collapseNext(collapsePast(handle));
    }

    private long collapsePast(long handle) {
        for (;;) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long pastRun = getAvailRunByOffset(runOffset - 1);
            if (pastRun == -1) {
                return handle;
            }

            int pastOffset = runOffset(pastRun);
            int pastPages = runPages(pastRun);

            //is continuous
            if (pastRun != handle && pastOffset + pastPages == runOffset) {
                //remove past run
                removeAvailRun(pastRun);
                handle = toRunHandle(pastOffset, pastPages + runPages, 0);
            } else {
                return handle;
            }
        }
    }

    private long collapseNext(long handle) {
        for (;;) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long nextRun = getAvailRunByOffset(runOffset + runPages);
            if (nextRun == -1) {
                return handle;
            }

            int nextOffset = runOffset(nextRun);
            int nextPages = runPages(nextRun);

            //is continuous
            if (nextRun != handle && runOffset + runPages == nextOffset) {
                //remove next run
                removeAvailRun(nextRun);
                handle = toRunHandle(runOffset, runPages + nextPages, 0);
            } else {
                return handle;
            }
        }
    }

    private static long toRunHandle(int runOffset, int runPages, int inUsed) {
        return (long) runOffset << RUN_OFFSET_SHIFT
               | (long) runPages << SIZE_SHIFT
               | (long) inUsed << IS_USED_SHIFT;
    }

    void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
                 PoolThreadCache threadCache) {
        if (isSubpage(handle)) {
            initBufWithSubpage(buf, nioBuffer, handle, reqCapacity, threadCache);
        } else {
            int maxLength = runSize(pageShifts, handle);
            buf.init(this, nioBuffer, handle, runOffset(handle) << pageShifts,
                    reqCapacity, maxLength, arena.parent.threadCache());
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
                            PoolThreadCache threadCache) {
        // 用int强转，取handle的低32位，低32位存储的是二叉树节点的位置
        int runOffset = runOffset(handle);
        // 右移32位并强制转化为int型，相当于获取handle的高32位。
        // 即PoolSubpage的内存段相对于page的偏移量
        int bitmapIdx = bitmapIdx(handle);

        PoolSubpage<T> s = subpages[runOffset];
        assert s.doNotDestroy;
        assert reqCapacity <= s.elemSize : reqCapacity + "<=" + s.elemSize;

        int offset = (runOffset << pageShifts) + bitmapIdx * s.elemSize;
        //计算偏移量，offset的值为内存对齐偏移量
        buf.init(this, nioBuffer, handle, offset, reqCapacity, s.elemSize, threadCache);
    }

    void incrementPinnedMemory(int delta) {
        assert delta > 0;
        pinnedBytes.add(delta);
    }

    void decrementPinnedMemory(int delta) {
        assert delta > 0;
        pinnedBytes.add(-delta);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        synchronized (arena) {
            return freeBytes;
        }
    }

    public int pinnedBytes() {
        return (int) pinnedBytes.value();
    }

    @Override
    public String toString() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }

    static int runOffset(long handle) {
        return (int) (handle >> RUN_OFFSET_SHIFT);
    }

    static int runSize(int pageShifts, long handle) {
        return runPages(handle) << pageShifts;
    }

    static int runPages(long handle) {
        return (int) (handle >> SIZE_SHIFT & 0x7fff);
    }

    static boolean isUsed(long handle) {
        return (handle >> IS_USED_SHIFT & 1) == 1L;
    }

    static boolean isRun(long handle) {
        return !isSubpage(handle);
    }

    static boolean isSubpage(long handle) {
        return (handle >> IS_SUBPAGE_SHIFT & 1) == 1L;
    }

    static int bitmapIdx(long handle) {
        return (int) handle;
    }
}
