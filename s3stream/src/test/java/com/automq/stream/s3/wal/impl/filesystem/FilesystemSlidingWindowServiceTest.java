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
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.block.Block;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class FilesystemSlidingWindowServiceTest {

    @Test
    void sealAndPollKeepsBlockOrderAndOffsets() {
        FilesystemSlidingWindowService service = new FilesystemSlidingWindowService(1024, 1024);
        Lock lock = service.getBlockLock();

        CompletableFuture<AppendResult.CallbackResult> future1 = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> future2 = new CompletableFuture<>();

        long offset1;
        long offset2;
        lock.lock();
        try {
            Block block = service.getCurrentBlockLocked();
            offset1 = addRecord(block, 100, future1);
            Block next = service.sealAndNewBlockLocked(block);
            offset2 = addRecord(next, 200, future2);
        } catch (OverCapacityException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        assertEquals(0, offset1);
        assertEquals(RECORD_HEADER_SIZE + 100, offset2);

        Block firstPolled = service.pollBlock();
        Block secondPolled = service.pollBlock();
        try {
            assertEquals(0, firstPolled.startOffset());
            assertEquals(RECORD_HEADER_SIZE + 100, firstPolled.size());
            assertEquals(RECORD_HEADER_SIZE + 100, secondPolled.startOffset());
            assertEquals(RECORD_HEADER_SIZE + 200, secondPolled.size());
            assertNull(service.pollBlock());
        } finally {
            firstPolled.release();
            secondPolled.release();
        }
    }

    @Test
    void pollCurrentBlockCreatesNextEmptyBlock() {
        FilesystemSlidingWindowService service = new FilesystemSlidingWindowService(2048, 2048);
        Lock lock = service.getBlockLock();
        CompletableFuture<AppendResult.CallbackResult> future = new CompletableFuture<>();
        long expectedNextOffset;
        lock.lock();
        try {
            Block current = service.getCurrentBlockLocked();
            addRecord(current, 128, future);
            expectedNextOffset = current.startOffset() + current.size();
        } finally {
            lock.unlock();
        }

        Block polled = service.pollBlock();
        try {
            assertEquals(0, polled.startOffset());
            assertEquals(expectedNextOffset, polled.endOffset());
        } finally {
            polled.release();
        }

        lock.lock();
        try {
            assertEquals(expectedNextOffset, service.nextAppendOffsetLocked());
        } finally {
            lock.unlock();
        }
        assertNull(service.pollBlock());
    }

    @Test
    void resetAndDrainPendingFuturesCollectOutstandingRequests() {
        FilesystemSlidingWindowService service = new FilesystemSlidingWindowService(4096, 4096);
        Lock lock = service.getBlockLock();
        CompletableFuture<AppendResult.CallbackResult> pendingFuture = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> currentFuture = new CompletableFuture<>();
        lock.lock();
        try {
            Block current = service.getCurrentBlockLocked();
            addRecord(current, 64, pendingFuture);
            Block next = service.sealAndNewBlockLocked(current);
            addRecord(next, 64, currentFuture);
        } catch (OverCapacityException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        Collection<CompletableFuture<AppendResult.CallbackResult>> futures = service.drainPendingFutures();
        assertEquals(2, futures.size());
        assertTrue(futures.contains(pendingFuture));
        assertTrue(futures.contains(currentFuture));

        service.resetTo(12345);
        lock.lock();
        try {
            assertEquals(12345, service.nextAppendOffsetLocked());
        } finally {
            lock.unlock();
        }
    }

    @Test
    void blockSoftLimitAppliesForNonEmptyBlock() {
        FilesystemSlidingWindowService service = new FilesystemSlidingWindowService(4096, 128);
        Lock lock = service.getBlockLock();
        CompletableFuture<AppendResult.CallbackResult> firstFuture = new CompletableFuture<>();
        CompletableFuture<AppendResult.CallbackResult> secondFuture = new CompletableFuture<>();
        long firstOffset;
        long secondOffset;
        lock.lock();
        try {
            Block block = service.getCurrentBlockLocked();
            firstOffset = addRecord(block, 64, firstFuture);
            secondOffset = addRecord(block, 64, secondFuture);
        } finally {
            lock.unlock();
        }
        assertEquals(0, firstOffset);
        assertEquals(-1, secondOffset);
        assertTrue(!secondFuture.isDone());
    }

    private static long addRecord(Block block, int bodySize, CompletableFuture<AppendResult.CallbackResult> future) {
        ByteBuf body = Unpooled.buffer(bodySize);
        body.writerIndex(bodySize);
        return block.addRecord(RECORD_HEADER_SIZE + bodySize, (offset, header) ->
            WALUtil.generateRecord(body, header, 0, offset), future);
    }
}
