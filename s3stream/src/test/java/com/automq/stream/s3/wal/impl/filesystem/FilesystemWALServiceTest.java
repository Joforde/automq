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
import com.automq.stream.s3.wal.util.WALUtil;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class FileSystemWALServiceTest {

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

    @Test
    void appendRolloverAndRecover() throws Exception {
        Path dir = createTempDir();
        try {
            // 64 KiB segments -> records easily span multiple files.
            long segmentSize = WALUtil.alignLargeByBlockSize(64L << 10);
            int recordSize = 4096 + 1;
            int recordCount = 50;

            WriteAheadLog wal = FileSystemWALService.builder(dir.toString(), segmentSize, 8L << 20)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
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

            // We expect rollover to have produced more than one segment file.
            File[] segmentFiles = dir.toFile().listFiles();
            assertNotNull(segmentFiles);
            long walFiles = 0;
            for (File f : segmentFiles) {
                if (Segment.parseSegmentIndex(f.getName()) >= 0) {
                    walFiles++;
                }
            }
            assertTrue(walFiles > 1, "expected rollover but only " + walFiles + " segment file(s) created");

            wal.shutdownGracefully();

            // Reopen and recover - all records should be readable.
            WriteAheadLog wal2 = FileSystemWALService.builder(dir.toString(), segmentSize, 8L << 20)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
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
                    byte[] expected = payloads.get(idx);
                    assertEquals(Unpooled.wrappedBuffer(expected), Unpooled.wrappedBuffer(bytes),
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

    @Test
    void trimReclaimsOldSegments() throws Exception {
        Path dir = createTempDir();
        try {
            long segmentSize = WALUtil.alignLargeByBlockSize(64L << 10);
            int recordSize = 4096 + 1;
            int recordCount = 40;

            WriteAheadLog wal = FileSystemWALService.builder(dir.toString(), segmentSize, 8L << 20)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
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

            long fileCountBefore = wal.metadata() == null ? 0
                : countWalFiles(dir);
            // Pick an offset past several segments so that early segments can be reclaimed.
            long trimTo = appendResults.get(recordCount - 1).recordOffset() - RECORD_HEADER_SIZE - 1;
            wal.trim(trimTo).join();

            // The trim background task is asynchronous; give it a brief chance to delete files.
            for (int i = 0; i < 50; i++) {
                if (countWalFiles(dir) < fileCountBefore) {
                    break;
                }
                Thread.sleep(20);
            }
            assertTrue(countWalFiles(dir) < fileCountBefore,
                "expected some segments to be deleted after trim");

            wal.shutdownGracefully();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    private static long countWalFiles(Path dir) {
        File[] files = dir.toFile().listFiles();
        if (files == null) {
            return 0;
        }
        long count = 0;
        for (File f : files) {
            if (Segment.parseSegmentIndex(f.getName()) >= 0) {
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
}
