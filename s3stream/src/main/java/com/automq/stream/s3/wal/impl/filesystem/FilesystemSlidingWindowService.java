/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.wal.impl.filesystem;

import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.common.BatchedBlockingQueue;
import com.automq.stream.s3.wal.common.BlockingMpscQueue;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.block.Block;
import com.automq.stream.s3.wal.impl.block.BlockImpl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A SlidingWindowService analogue dedicated to the file-system WAL implementation.
 * <p>
 * It manages an in-memory pipeline of {@link Block}s in front of the on-disk segment files:
 * <ul>
 *   <li>{@code currentBlock} - mutable block records are appended to; protected by {@link #blockLock}.</li>
 *   <li>{@code pendingBlocks} - sealed blocks waiting to be picked up by the block-poll thread.</li>
 * </ul>
 * Unlike {@link com.automq.stream.s3.wal.impl.block.SlidingWindowService} which manages a fixed
 * size circular buffer, this service is unbounded: blocks are sealed when their size reaches a
 * configurable limit and then drained sequentially in append order by a dedicated thread.
 */
public class FilesystemSlidingWindowService {

    /**
     * Default capacity of the pending blocks queue.
     */
    private static final int DEFAULT_PENDING_BLOCKS_CAPACITY = 10;

    private final long blockMaxSize;
    private final long blockSoftLimit;

    private final Lock blockLock = new ReentrantLock();
    private final BatchedBlockingQueue<Block> pendingBlocks;

    private volatile Block currentBlock;
    /**
     * Start offset of the next block to be created. Always updated under {@link #blockLock}.
     */
    private long nextStartOffset;

    public FilesystemSlidingWindowService(long blockMaxSize, long blockSoftLimit) {
        this(blockMaxSize, blockSoftLimit, DEFAULT_PENDING_BLOCKS_CAPACITY);
    }

    public FilesystemSlidingWindowService(long blockMaxSize, long blockSoftLimit, int pendingBlocksCapacity) {
        if (blockMaxSize <= 0) {
            throw new IllegalArgumentException("blockMaxSize must be positive: " + blockMaxSize);
        }
        if (blockSoftLimit <= 0 || blockSoftLimit > blockMaxSize) {
            throw new IllegalArgumentException("invalid blockSoftLimit: " + blockSoftLimit + ", blockMaxSize: " + blockMaxSize);
        }
        if (pendingBlocksCapacity <= 0) {
            throw new IllegalArgumentException("pendingBlocksCapacity must be positive: " + pendingBlocksCapacity);
        }
        this.blockMaxSize = blockMaxSize;
        this.blockSoftLimit = blockSoftLimit;
        this.pendingBlocks = new BlockingMpscQueue<>(pendingBlocksCapacity);
    }

    /**
     * The lock guarding {@link #currentBlock} and {@link #pendingBlocks}. Callers that interact
     * with the current block (e.g. {@link #getCurrentBlockLocked()},
     * {@link #sealAndNewBlockLocked(Block)}) must acquire this lock first.
     */
    public Lock getBlockLock() {
        return blockLock;
    }

    /**
     * Approximate number of sealed blocks waiting for the block-poll thread.
     */
    public int pendingBlocksQueueSize() {
        return pendingBlocks.size();
    }

    /**
     * Return the current block, creating it lazily if necessary.
     * <p>
     * Note: this method is NOT thread-safe; the caller must hold {@link #blockLock}.
     */
    public Block getCurrentBlockLocked() {
        if (currentBlock == null) {
            currentBlock = newBlock(nextStartOffset);
        }
        return currentBlock;
    }

    /**
     * Seal the supplied previous block (if non-empty) and replace it with a fresh, empty block
     * starting immediately after the previous one.
     * <p>
     * Note: this method is NOT thread-safe; the caller must hold {@link #blockLock}.
     *
     * @throws OverCapacityException if the pending blocks queue is full, providing back-pressure
     *                               to the caller.
     */
    public Block sealAndNewBlockLocked(Block previousBlock) throws OverCapacityException {
        long newStart = previousBlock.startOffset() + previousBlock.size();
        Block newBlock = newBlock(newStart);
        if (!previousBlock.isEmpty()) {
            try {
                if (!pendingBlocks.offer(previousBlock, 10, TimeUnit.MILLISECONDS)) {
                    // Queue is full - release the new block and signal back-pressure.
                    newBlock.release();
                    throw new OverCapacityException("WAL pending blocks queue is full (capacity=" + (pendingBlocks.size() + pendingBlocks.remainingCapacity()) + ")");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            previousBlock.release();
        }
        nextStartOffset = newStart;
        currentBlock = newBlock;
        return newBlock;
    }

    /**
     * Try to retrieve the next block ready to be written to disk.
     * <p>
     * Resolution order:
     * <ol>
     *   <li>The oldest entry in {@link #pendingBlocks}.</li>
     *   <li>The current block, if it contains at least one record. The current block is then
     *   replaced with a new empty one that continues the offset sequence.</li>
     * </ol>
     * Returns {@code null} when there is nothing to write.
     */
    public Block pollBlock() {
        blockLock.lock();
        try {
            Block polled = pendingBlocks.poll();
            if (polled != null) {
                return polled;
            }
            Block cur = currentBlock;
            if (cur != null && !cur.isEmpty()) {
                long newStart = cur.startOffset() + cur.size();
                currentBlock = newBlock(newStart);
                nextStartOffset = newStart;
                return cur;
            }
            return null;
        } finally {
            blockLock.unlock();
        }
    }

    /**
     * Logical offset that will be assigned to the next record handed to the current block.
     * <p>
     * Note: this method is NOT thread-safe; the caller must hold {@link #blockLock}.
     */
    public long nextAppendOffsetLocked() {
        Block cur = getCurrentBlockLocked();
        return cur.startOffset() + cur.size();
    }

    /**
     * Reset the pipeline so that the next block starts at {@code startOffset}. All in-flight
     * data is released and pending blocks are dropped; intended for reset-style flows
     * where any not-yet-durable data has been logically discarded by the caller.
     */
    public void resetTo(long startOffset) {
        blockLock.lock();
        try {
            for (Block b : pendingBlocks) {
                b.release();
            }
            pendingBlocks.clear();
            if (currentBlock != null) {
                currentBlock.release();
                currentBlock = null;
            }
            nextStartOffset = startOffset;
        } finally {
            blockLock.unlock();
        }
    }

    /**
     * Collect every {@link CompletableFuture} owned by blocks still resident in the pipeline.
     * Used during shutdown to fail outstanding appends.
     */
    public Collection<CompletableFuture<AppendResult.CallbackResult>> drainPendingFutures() {
        Collection<CompletableFuture<AppendResult.CallbackResult>> futures = new LinkedList<>();
        blockLock.lock();
        try {
            for (Block block : pendingBlocks) {
                futures.addAll(block.futures());
                block.release();
            }
            pendingBlocks.clear();
            if (currentBlock != null && !currentBlock.isEmpty()) {
                futures.addAll(currentBlock.futures());
            }
            if (currentBlock != null) {
                currentBlock.release();
                currentBlock = null;
            }
        } finally {
            blockLock.unlock();
        }
        return futures;
    }

    private Block newBlock(long startOffset) {
        return new BlockImpl(startOffset, blockMaxSize, blockSoftLimit);
    }
}
