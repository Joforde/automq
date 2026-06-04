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

    /**
     * Capacity of the in-memory merge buffer. Small {@link #write(ByteBuf)} calls are copied into
     * this buffer and flushed to the channel in a single, larger {@link FileChannel#write}, which
     * avoids the poor performance of issuing one syscall per small record.
     */
    private static final int WRITE_BUFFER_CAPACITY = 1 << 20;

    private final long startWalOffset;
    private final File file;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    /**
     * Logical end position of this file (0-based), i.e. the number of bytes that have been handed
     * to {@link #write(ByteBuf)}, including bytes still sitting in {@link #writeBuffer} that have
     * not yet been pushed to the channel. Mutated only by the single WAL write thread.
     */
    private long writePosition;
    /**
     * In-memory buffer that merges consecutive small writes before they are flushed to the channel.
     * Incoming records are copied here and only written out once the buffer fills up or an explicit
     * {@link #flush()} / {@link #fsync()} happens.
     */
    private final ByteBuf writeBuffer;
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
        // Position the channel at the end of the file so buffered data is appended on flush.
        this.fileChannel.position(this.writePosition);
        this.writeBuffer = ByteBufAlloc.byteBuffer(WRITE_BUFFER_CAPACITY);
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
        writeBuffer.release();
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
        int len = src.readableBytes();
        if (len <= 0) {
            return;
        }
        int copied = 0;
        while (copied < len) {
            int bytesToCopy = Math.min(len - copied, writeBuffer.writableBytes());
            // Absolute-index copy so the caller's reader index is left untouched.
            writeBuffer.writeBytes(src, src.readerIndex() + copied, bytesToCopy);
            copied += bytesToCopy;
            // Flush eagerly when the merge buffer is full so it never grows past its capacity.
            if (!writeBuffer.isWritable()) {
                flush();
            }
        }
        writePosition += copied;
    }

    /**
     * Push any data buffered in {@link #writeBuffer} to the underlying channel. The data is appended
     * at the current channel position; it is not durable until {@link #fsync()} is called.
     */
    public synchronized void flush() throws IOException {
        if (writeBuffer.writerIndex() == 0) {
            return;
        }
        ByteBuffer toWrite = writeBuffer.internalNioBuffer(0, writeBuffer.writerIndex());
        while (toWrite.hasRemaining()) {
            int n = fileChannel.write(toWrite);
            if (n < 0) {
                throw new IOException("write returned " + n + " on " + file);
            }
        }
        writeBuffer.clear();
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
        // Ensure buffered-but-not-yet-flushed bytes are visible to positional reads.
        flush();
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
        // Flush merged writes before forcing so the durability point reflects all appended data.
        flush();
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
