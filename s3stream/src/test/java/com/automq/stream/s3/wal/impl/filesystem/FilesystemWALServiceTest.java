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

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class FilesystemWALServiceTest {

    /**
     * Small segment roll threshold used in multi-segment tests.
     * Each record body is 4096 bytes; with RECORD_HEADER_SIZE overhead each record
     * occupies slightly more than 4 KiB, so 4 records fill roughly 16-17 KiB.
     * A threshold of 20 000 bytes therefore triggers a roll every ~4 records.
     */
    private static final long SMALL_SEGMENT_THRESHOLD = 20_000L;
    private static final int RECORD_BODY_SIZE = 4096;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Path createTempDir() throws IOException {
        return Files.createTempDirectory("file-wal-test-");
    }

    private static void recoverAndReset(WriteAheadLog wal) {
        Iterator<RecoverResult> it = wal.recover();
        while (it.hasNext()) {
            it.next().record().release();
        }
        wal.reset().join();
    }

    /** Count WAL segment files (wal-*.log) in the given directory. */
    private static long countWalFiles(Path dir) {
        File[] files = dir.toFile().listFiles();
        if (files == null) {
            return 0;
        }
        long count = 0;
        for (File f : files) {
            if (Segment.parseStartOffset(f.getName()) >= 0) {
                count++;
            }
        }
        return count;
    }

    private static void deleteRecursive(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        file.delete();
    }

    /**
     * Poll for a condition up to {@code maxWaitMs} ms in 20 ms increments.
     */
    private static void waitForCondition(BooleanSupplierWithInterrupt condition, long maxWaitMs)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (!condition.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    interface BooleanSupplierWithInterrupt {
        boolean get() throws InterruptedException;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /**
     * Verify that {@link WalMetadataFile} is created when the WAL starts, before any data segment exists.
     */
    @Test
    void walMetaFileCreatedOnStart() throws Exception {
        Path dir = createTempDir();
        try {
            WriteAheadLog wal = FilesystemWALService.builder(dir.toString()).build().start();
            assertTrue(new File(dir.toFile(), WalMetadataFile.FILE_NAME).isFile(),
                "wal.meta should exist after start");
            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Verify that a segment file is created on disk after the first record is appended.
     * Segments are allocated lazily on the first write, not at WAL startup.
     */
    @Test
    void initialSegmentCreatedOnFirstAppend() throws Exception {
        Path dir = createTempDir();
        try {
            WriteAheadLog wal = FilesystemWALService.builder(dir.toString()).build().start();
            recoverAndReset(wal);

            assertEquals(0, countWalFiles(dir), "no segment file should exist before the first write");

            ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
            wal.append(data).future().join();

            long fileCount = countWalFiles(dir);
            assertTrue(fileCount >= 1,
                "expected at least one segment file after the first write, found: " + fileCount);

            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Verify that segment files use the expected naming convention (wal-<20-digit-offset>.log)
     * and that the first segment has start offset 0.
     */
    @Test
    void segmentFileNameFormat() throws Exception {
        Path dir = createTempDir();
        try {
            WriteAheadLog wal = FilesystemWALService.builder(dir.toString()).build().start();
            recoverAndReset(wal);

            // Write one record so that a segment is created.
            ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
            wal.append(data).future().join();

            File[] files = dir.toFile().listFiles();
            assertTrue(files != null && files.length > 0, "no segment files found");

            boolean foundZeroOffset = false;
            for (File f : files) {
                long offset = Segment.parseStartOffset(f.getName());
                if (offset < 0) {
                    continue;
                }
                // File name must be exactly "wal-<20-digit-zero-padded-offset>.log"
                String expected = String.format("wal-%020d.log", offset);
                assertEquals(expected, f.getName(), "segment file name does not match expected pattern");
                if (offset == 0) {
                    foundZeroOffset = true;
                }
            }
            assertTrue(foundZeroOffset, "expected first segment to start at offset 0");

            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Write enough records to exceed the roll threshold several times and verify that multiple
     * segment files are created on disk.
     */
    @Test
    void segmentRollCreatesNewFiles() throws Exception {
        Path dir = createTempDir();
        try {
            // 30 records × ~4 KiB each = ~120 KiB; with a 20 KiB threshold we expect ≥ 5 segments.
            int recordCount = 30;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            recoverAndReset(wal);

            List<CompletableFuture<AppendResult.CallbackResult>> futures = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
                futures.add(wal.append(data).future());
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            long fileCount = countWalFiles(dir);
            assertTrue(fileCount > 1,
                "expected more than one segment file after " + recordCount + " records with threshold "
                    + SMALL_SEGMENT_THRESHOLD + " bytes, got: " + fileCount);

            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Write 50 records across multiple segments, shut down normally, reopen and verify that
     * every record (offset and payload) is recovered in order.
     */
    @Test
    void appendAndRecover() throws Exception {
        Path dir = createTempDir();
        try {
            int recordSize = 4096 + 1;
            int recordCount = 50;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .build()
                .start();
            recoverAndReset(wal);

            List<Long> offsets = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();
            List<CompletableFuture<AppendResult.CallbackResult>> futures = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);
                byte[] copy = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), copy);
                payloads.add(copy);
                AppendResult result = wal.append(data.retainedDuplicate());
                data.release();
                offsets.add(result.recordOffset());
                futures.add(result.future());
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            wal.shutdownGracefully();

            WriteAheadLog wal2 = FilesystemWALService.builder(dir.toString())
                .build()
                .start();
            try {
                Iterator<RecoverResult> it = wal2.recover();
                int idx = 0;
                while (it.hasNext()) {
                    RecoverResult result = it.next();
                    assertEquals(offsets.get(idx).longValue(), result.recordOffset(),
                        "record offset mismatch at index " + idx);
                    byte[] bytes = new byte[result.record().readableBytes()];
                    result.record().getBytes(result.record().readerIndex(), bytes);
                    result.record().release();
                    assertEquals(Unpooled.wrappedBuffer(payloads.get(idx)), Unpooled.wrappedBuffer(bytes),
                        "record body mismatch at index " + idx);
                    idx++;
                }
                assertEquals(recordCount, idx, "recovered record count mismatch");
                wal2.reset().join();
            } finally {
                wal2.shutdownGracefully();
            }
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Simulate an ungraceful shutdown (no call to {@code shutdownGracefully}) and verify that
     * recover still returns all previously written records.
     */
    @Test
    void recoverAfterUngracefulShutdown() throws Exception {
        Path dir = createTempDir();
        try {
            int recordCount = 20;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            recoverAndReset(wal);

            List<Long> offsets = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();
            List<CompletableFuture<AppendResult.CallbackResult>> futures = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
                byte[] copy = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), copy);
                payloads.add(copy);
                AppendResult result = wal.append(data.retainedDuplicate());
                data.release();
                offsets.add(result.recordOffset());
                futures.add(result.future());
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Deliberately skip shutdownGracefully – simulates a crash.
            // (Thread pools are daemon threads so they do not block JVM exit.)

            WriteAheadLog wal2 = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            try {
                Iterator<RecoverResult> it = wal2.recover();
                int idx = 0;
                while (it.hasNext()) {
                    RecoverResult result = it.next();
                    assertEquals(offsets.get(idx).longValue(), result.recordOffset(),
                        "record offset mismatch at index " + idx);
                    byte[] bytes = new byte[result.record().readableBytes()];
                    result.record().getBytes(result.record().readerIndex(), bytes);
                    result.record().release();
                    assertEquals(Unpooled.wrappedBuffer(payloads.get(idx)), Unpooled.wrappedBuffer(bytes),
                        "record body mismatch at index " + idx);
                    idx++;
                }
                assertEquals(recordCount, idx,
                    "all records must be recoverable after an ungraceful shutdown");
                wal2.reset().join();
            } finally {
                wal2.shutdownGracefully();
            }
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Write records that deliberately span multiple segment files (via a small roll threshold),
     * reopen, and verify that every record is recovered correctly across the segment boundary.
     */
    @Test
    void recoverAcrossMultipleSegments() throws Exception {
        Path dir = createTempDir();
        try {
            int recordCount = 30;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            recoverAndReset(wal);

            List<Long> offsets = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();
            List<CompletableFuture<AppendResult.CallbackResult>> futures = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
                byte[] copy = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), copy);
                payloads.add(copy);
                AppendResult result = wal.append(data.retainedDuplicate());
                data.release();
                offsets.add(result.recordOffset());
                futures.add(result.future());
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            long segmentCount = countWalFiles(dir);
            assertTrue(segmentCount > 1,
                "test precondition: expected multiple segment files, got: " + segmentCount);

            wal.shutdownGracefully();

            // Reopen with the same threshold so rollover logic is consistent.
            WriteAheadLog wal2 = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            try {
                Iterator<RecoverResult> it = wal2.recover();
                int idx = 0;
                while (it.hasNext()) {
                    RecoverResult result = it.next();
                    assertEquals(offsets.get(idx).longValue(), result.recordOffset(),
                        "record offset mismatch at index " + idx);
                    byte[] bytes = new byte[result.record().readableBytes()];
                    result.record().getBytes(result.record().readerIndex(), bytes);
                    result.record().release();
                    assertEquals(Unpooled.wrappedBuffer(payloads.get(idx)), Unpooled.wrappedBuffer(bytes),
                        "record body mismatch at index " + idx);
                    idx++;
                }
                assertEquals(recordCount, idx,
                    "all records must be recoverable when spread across multiple segments");
                wal2.reset().join();
            } finally {
                wal2.shutdownGracefully();
            }
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Verify that trim reclaims old segment files that are fully below the trim offset.
     */
    @Test
    void trimReclaimsOldSegments() throws Exception {
        Path dir = createTempDir();
        try {
            int recordSize = 4096 + 1;
            int recordCount = 40;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(42_000L)
                .build()
                .start();
            recoverAndReset(wal);

            List<AppendResult> appendResults = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);
                appendResults.add(wal.append(data));
            }
            for (AppendResult r : appendResults) {
                r.future().join();
            }

            long fileCountBefore = countWalFiles(dir);
            assertTrue(fileCountBefore > 1, "test precondition: expected multiple segment files");

            // Trim to just before the last record so that all earlier segments can be reclaimed.
            long trimTo = appendResults.get(recordCount - 1).recordOffset() - RECORD_HEADER_SIZE - 1;
            wal.trim(trimTo).join();

            waitForCondition(() -> countWalFiles(dir) < fileCountBefore, 1_000);
            assertTrue(countWalFiles(dir) < fileCountBefore,
                "expected some segments to be deleted after trim, before=" + fileCountBefore
                    + " after=" + countWalFiles(dir));

            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Verify that the last remaining segment file is never deleted by trim, ensuring trim
     * metadata is always durable regardless of how far the trim offset advances.
     */
    @Test
    void trimPreservesLastSegment() throws Exception {
        Path dir = createTempDir();
        try {
            int recordCount = 30;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            recoverAndReset(wal);

            List<AppendResult> results = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
                results.add(wal.append(data));
            }
            for (AppendResult r : results) {
                r.future().join();
            }

            long segmentCountBefore = countWalFiles(dir);
            assertTrue(segmentCountBefore > 1, "test precondition: expected multiple segment files");

            // Trim past the end of all written data; all but the very last segment should be deleted.
            long lastRecordOffset = results.get(recordCount - 1).recordOffset();
            long trimTo = lastRecordOffset + RECORD_BODY_SIZE + RECORD_HEADER_SIZE;
            wal.trim(trimTo).join();

            // Wait until no more deletions happen.
            waitForCondition(() -> countWalFiles(dir) == 1, 2_000);
            assertEquals(1, countWalFiles(dir),
                "the last segment must never be deleted by trim, even when the trim offset exceeds all data");

            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    /**
     * Verify that after trim, trim offset is persisted in {@link WalMetadataFile} so recovery
     * starts from the right position
     * and does not replay already-trimmed records.
     */
    @Test
    void trimOffsetPersistedAndRespectedOnRecover() throws Exception {
        Path dir = createTempDir();
        try {
            int recordCount = 20;

            WriteAheadLog wal = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            recoverAndReset(wal);

            List<AppendResult> results = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(RECORD_BODY_SIZE);
                byte[] copy = new byte[data.readableBytes()];
                data.getBytes(data.readerIndex(), copy);
                payloads.add(copy);
                results.add(wal.append(data.retainedDuplicate()));
                data.release();
            }
            for (AppendResult r : results) {
                r.future().join();
            }

            // Trim the first half of the records.
            int trimIdx = recordCount / 2;
            long trimTo = results.get(trimIdx).recordOffset() - 1;
            wal.trim(trimTo).join();
            wal.shutdownGracefully();

            // Reopen: recovery must only return records from trimIdx onward.
            WriteAheadLog wal2 = FilesystemWALService.builder(dir.toString())
                .segmentRollThresholdBytes(SMALL_SEGMENT_THRESHOLD)
                .build()
                .start();
            try {
                Iterator<RecoverResult> it = wal2.recover();
                int idx = trimIdx;
                while (it.hasNext()) {
                    RecoverResult result = it.next();
                    assertTrue(idx < recordCount,
                        "recovery returned more records than expected (got record at idx=" + idx + ")");
                    assertEquals(results.get(idx).recordOffset(), result.recordOffset(),
                        "recovered offset mismatch at idx " + idx);
                    byte[] bytes = new byte[result.record().readableBytes()];
                    result.record().getBytes(result.record().readerIndex(), bytes);
                    result.record().release();
                    assertEquals(Unpooled.wrappedBuffer(payloads.get(idx)), Unpooled.wrappedBuffer(bytes),
                        "recovered payload mismatch at idx " + idx);
                    idx++;
                }
                assertEquals(recordCount, idx,
                    "expected to recover records from trimIdx=" + trimIdx + " to end");
                wal2.reset().join();
            } finally {
                wal2.shutdownGracefully();
            }
        } finally {
            deleteRecursive(dir.toFile());
        }
    }
}
