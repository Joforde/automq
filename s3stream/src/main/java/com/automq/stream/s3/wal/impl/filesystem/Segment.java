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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A single WAL segment file using append-only writes. The file contains only record bytes; WAL
 * metadata is stored in {@link WalMetadataFile}.
 * <p>
 * Each segment owns a contiguous range of global record offsets starting from {@code startOffset}.
 * Writes are issued directly against the underlying {@link FileChannel}; durability is provided
 * by {@link #fsync()}.
 */
public class Segment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);

    private final long startWalOffset;
    private final File file;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    /**
     * Next byte position to write in this file (0-based). Mutated only by the single WAL write
     * thread, so no synchronization is required for the writer; readers use
     * {@link FileChannel#read(ByteBuffer, long)} which is unaffected by the channel position.
     */
    private long writePosition;
    /**
     * Next byte position to read in this file (0-based), for sequential reads.
     */
    private long nextReadPosition;
    private volatile boolean closed;

    public Segment(File file, long startWalOffset) throws IOException {
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
        this.raf = new RandomAccessFile(file, "rw");
        this.fileChannel = raf.getChannel();
        this.writePosition = raf.length();
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
     */
    public synchronized long endOffsetExclusive() {
        return startWalOffset + writePosition;
    }

    public File file() {
        return file;
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
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
     * Delete the underlying file. The segment is closed first.
     */
    public synchronized void deleteQuietly() {
        close();
        if (file.exists() && !file.delete()) {
            LOGGER.warn("failed to delete segment file {}", file);
        }
    }

    /**
     * Append record bytes to the end of this segment. Not durable until {@link #fsync()} is
     * called.
     * <p>
     * This method must only be invoked from a single writer thread.
     */
    public synchronized void write(ByteBuf src) throws IOException {
        if (closed) {
            throw new IOException("segment already closed: " + file);
        }
        int total = src.readableBytes();
        if (total <= 0) {
            return;
        }
        ByteBuffer[] buffers = src.nioBuffers();
        long position = writePosition;
        long written = 0;
        // Use positional pwrite calls so writes are independent of the channel position.
        for (ByteBuffer buffer : buffers) {
            while (buffer.hasRemaining()) {
                int n = fileChannel.write(buffer, position + written);
                if (n < 0) {
                    throw new IOException("write returned " + n + " on " + file);
                }
                written += n;
            }
        }
        writePosition = position + written;
    }

    /**
     * Read {@code length} bytes sequentially from the start of this segment file, appending them to
     * {@code dst}. Each call advances an internal read cursor.
     */
    public synchronized int read(ByteBuf dst, int length) throws IOException {
        int n = readAt(dst, startWalOffset + nextReadPosition, length);
        if (n > 0) {
            nextReadPosition += n;
        }
        return n;
    }

    /**
     * Read {@code length} bytes starting at the given WAL logical offset, appending them to {@code dst}.
     * Used during WAL recovery where records are addressed by global offset.
     */
    public synchronized int readAt(ByteBuf dst, long walLogicalOffset, int length) throws IOException {
        if (closed) {
            throw new IOException("segment already closed: " + file);
        }
        long position = positionForOffset(walLogicalOffset);
        int toRead = Math.min(length, dst.writableBytes());
        if (toRead <= 0) {
            return 0;
        }
        int total = 0;
        while (total < toRead) {
            int read = dst.writeBytes(fileChannel, position + total, toRead - total);
            if (read < 0) {
                break;
            }
            if (read == 0) {
                // No more data available at the requested position.
                break;
            }
            total += read;
        }
        return total;
    }

    /**
     * Force pending data to disk.
     */
    public synchronized void fsync() throws IOException {
        if (closed) {
            throw new IOException("segment already closed: " + file);
        }
        fileChannel.force(false);
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
}
