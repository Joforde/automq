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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * A single WAL segment file using append-only writes. The file contains only record bytes; WAL
 * metadata is stored in {@link WalMetadataFile}.
 * <p>
 * Each segment owns a contiguous range of global record offsets starting from {@code startOffset}.
 */
public class Segment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);

    /**
     * Default capacity (bytes) of the per-segment {@link BufferedChannel} write buffer.
     */
    public static final int DEFAULT_WRITE_BUFFER_CAPACITY = 64 << 10;
    private static final int WRITE_BUFFER = DEFAULT_WRITE_BUFFER_CAPACITY;
    private final long startWalOffset;
    private final File file;
    /**
     * Next byte position to write in this file (0-based). Also represents the current file size
     * for an append-only segment.
     */
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private BufferedChannel bc;
    /**
     * Next byte position to read in this file (0-based), for sequential reads.
     */
    private long nextReadPosition;

    public Segment(File file, long startWalOffset) throws IOException {
        this(file, startWalOffset, WRITE_BUFFER);
    }

    public Segment(File file, long startWalOffset, int writeBuffer) throws IOException {
        if (writeBuffer <= 0) {
            throw new IllegalArgumentException("writeBuffer must be positive: " + writeBuffer);
        }
        this.file = file;
        this.startWalOffset = startWalOffset;
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
        fileChannel = raf.getChannel();
        if (exists) {
            fileChannel.position(raf.length());
        }
        bc = new BufferedChannel(fileChannel, writeBuffer);
    }

    /**
     * Standard segment file name encoding: uses the startOffset as the file name identifier.
     */
    public static String buildFileName(long startOffset) {
        return String.format("wal-%020d.log", startOffset);
    }

    /**
     * Parse a segment file name back to its start offset. Returns -1 if it does not match the
     * pattern.
     */
    public static long parseStartOffset(String fileName) {
        if (!fileName.startsWith("wal-") || !fileName.endsWith(".log")) {
            return -1;
        }
        String middle = fileName.substring("wal-".length(), fileName.length() - ".log".length());
        if (middle.length() != 20) {
            return -1;
        }
        try {
            long v = Long.parseLong(middle);
            if (v < 0) {
                return -1;
            }
            return v;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    public long startOffset() {
        return startWalOffset;
    }

    /**
     * Returns the exclusive end offset of this segment based on actual bytes written.
     * This is {@code startOffset + nextWritePosition}.
     */
    public long endOffsetExclusive() {
        return startWalOffset + file.length();
    }

    public File file() {
        return file;
    }

    public synchronized void close() {
        if (bc == null) {
            return;
        }
        try {
            bc.close();
        } catch (IOException ignored) {
        }
        fileChannel = null;
        raf = null;
        bc = null;
    }

    /**
     * Delete the underlying file. The segment is closed first.
     */
    public void deleteQuietly() {
        close();
        if (file.exists() && !file.delete()) {
            LOGGER.warn("failed to delete segment file {}", file);
        }
    }

    /**
     * Append record bytes to the end of this segment.
     */
    public void write(ByteBuf src) throws IOException {
        bc.write(src);
    }

    /**
     * Read {@code length} bytes sequentially from the start of this segment file, appending them to
     * {@code dst}. Each call advances an internal read cursor.
     */
    public int read(ByteBuf dst, int length) throws IOException {
        length = Math.min(length, dst.writableBytes());
        int read = bc.read(dst, nextReadPosition, length);
        nextReadPosition += read;
        return read;
    }

    /**
     * Read {@code length} bytes starting at the given WAL logical offset, appending them to {@code dst}.
     * Used during WAL recovery where records are addressed by global offset.
     */
    public int readAt(ByteBuf dst, long walLogicalOffset, int length) throws IOException {
        long position = positionForOffset(walLogicalOffset);
        length = Math.min(length, dst.writableBytes());
        return bc.read(dst, position, length);
    }

    /**
     * Force pending data to disk.
     */
    public void fsync() throws IOException {
        bc.flushAndForceWrite(true);
    }



    public boolean containsOffset(long walLogicalOffset) {
        return walLogicalOffset >= startWalOffset && walLogicalOffset < endOffsetExclusive();
    }

    public long positionForOffset(long walLogicalOffset) {
        return walLogicalOffset - startWalOffset;
    }

    @Override
    public String toString() {
        return "Segment{startOffset=" + startWalOffset + ", file=" + file + '}';
    }

    public long getPosition() {
        return bc.position();
    }
}
