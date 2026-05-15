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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.exception.UnmarshalException;
import com.automq.stream.s3.wal.util.WALUtil;

import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dedicated on-disk store for {@link FilesystemWALHeader}. Lives at {@value #FILE_NAME} under the
 * WAL directory and stays open for the lifetime of {@link FilesystemWALService}, independent of
 * data segment trim/delete.
 */
public class WalMetadataFile {
    public static final String FILE_NAME = "wal.meta";
    private static final int HEADER_SLOT_COUNT = 2;
    private static final int HEADER_SLOT_CAPACITY = WALUtil.BLOCK_SIZE;
    private static final int FILE_CAPACITY = HEADER_SLOT_CAPACITY * HEADER_SLOT_COUNT;

    private final File file;
    private final AtomicLong writeRoundTimes = new AtomicLong(0);
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    public WalMetadataFile(File directory) {
        this.file = new File(directory, FILE_NAME);
    }

    public File file() {
        return file;
    }

    public boolean exists() {
        return file.isFile();
    }

    public void openOrCreate() throws IOException {
        boolean exists = file.exists();
        if (!exists) {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IOException("mkdirs " + parent + " failed");
            }
            if (!file.createNewFile()) {
                throw new IOException("create " + file + " failed");
            }
        }
        raf = new RandomAccessFile(file, "rw");
        if (raf.length() < FILE_CAPACITY) {
            raf.setLength(FILE_CAPACITY);
        }
        fileChannel = raf.getChannel();
    }

    public void close() {
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
        } catch (IOException ignored) {
        }
        try {
            if (raf != null) {
                raf.close();
            }
        } catch (IOException ignored) {
        }
        fileChannel = null;
        raf = null;
    }

    /**
     * Read both header slots and return whichever one is newer (by lastWriteTimestamp).
     */
    public FilesystemWALHeader readLatestHeader() throws IOException {
        ensureOpen();
        FilesystemWALHeader latest = null;
        for (int i = 0; i < HEADER_SLOT_COUNT; i++) {
            ByteBuf buf = ByteBufAlloc.byteBuffer(FilesystemWALHeader.WAL_HEADER_SIZE);
            try {
                long position = (long) i * HEADER_SLOT_CAPACITY;
                int total = 0;
                while (total < FilesystemWALHeader.WAL_HEADER_SIZE) {
                    int read = buf.writeBytes(fileChannel, position + total,
                        FilesystemWALHeader.WAL_HEADER_SIZE - total);
                    if (read == -1) {
                        break;
                    }
                    total += read;
                }
                if (total != FilesystemWALHeader.WAL_HEADER_SIZE) {
                    continue;
                }
                FilesystemWALHeader candidate = FilesystemWALHeader.unmarshal(buf);
                if (latest == null || latest.getLastWriteTimestamp() < candidate.getLastWriteTimestamp()) {
                    latest = candidate;
                }
            } catch (UnmarshalException ignored) {
                // Slot is empty/corrupted - try the other one.
            } finally {
                buf.release();
            }
        }
        return latest;
    }

    public synchronized void writeHeader(FilesystemWALHeader header) throws IOException {
        ensureOpen();
        long position = writeRoundTimes.getAndIncrement() % HEADER_SLOT_COUNT * HEADER_SLOT_CAPACITY;
        header.setLastWriteTimestamp(System.nanoTime());
        long trimOffset = header.getTrimOffset();
        ByteBuf buf = header.marshal();
        try {
            ByteBuffer[] nioBuffers = buf.nioBuffers();
            long currentPosition = position;
            for (ByteBuffer nioBuffer : nioBuffers) {
                while (nioBuffer.hasRemaining()) {
                    int written = fileChannel.write(nioBuffer, currentPosition);
                    if (written == -1) {
                        throw new IOException("write -1 at position " + currentPosition);
                    }
                    currentPosition += written;
                }
            }
            fileChannel.force(true);
        } finally {
            buf.release();
        }
        header.updateFlushedTrimOffset(trimOffset);
    }

    private void ensureOpen() throws IOException {
        if (fileChannel == null) {
            throw new IOException("wal metadata file is not open: " + file);
        }
    }
}
