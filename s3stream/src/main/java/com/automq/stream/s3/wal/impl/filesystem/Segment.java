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
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single WAL segment file using append-only writes. The file contains only record bytes; WAL
 * metadata is stored in {@link WalMetadataFile}.
 * <p>
 * Each segment owns a contiguous range of global record offsets starting from {@code startOffset}.
 */
public class Segment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);

    private final long startOffset;
    private final File file;
    /**
     * Next byte position to write in this file (0-based). Also represents the current file size
     * for an append-only segment.
     */
    private final AtomicLong nextWritePosition = new AtomicLong(0);
    private RandomAccessFile raf;
    private FileChannel fileChannel;

    public Segment(File file, long startOffset) {
        this.file = file;
        this.startOffset = startOffset;
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
        return startOffset;
    }

    /**
     * Returns the exclusive end offset of this segment based on actual bytes written.
     * This is {@code startOffset + nextWritePosition}.
     */
    public long endOffsetExclusive() {
        return startOffset + nextWritePosition.get();
    }

    public File file() {
        return file;
    }

    /**
     * Open (and create when necessary) the underlying file. For existing files, the write position
     * is restored from the file size.
     */
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
        fileChannel = raf.getChannel();

        if (exists) {
            nextWritePosition.set(raf.length());
        }
    }

    public boolean isOpen() {
        return fileChannel != null;
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
     * Delete the underlying file. The segment is closed first.
     */
    public void deleteQuietly() {
        close();
        if (file.exists() && !file.delete()) {
            LOGGER.warn("failed to delete segment file {}", file);
        }
    }

    /**
     * Write record bytes at the given WAL logical byte offset. The caller must guarantee that the offset
     * falls within this segment (or equals {@link #endOffsetExclusive()} for the next append).
     */
    public long writeAt(ByteBuf src, long walLogicalOffset) throws IOException {
        assert containsOffset(walLogicalOffset) || walLogicalOffset == endOffsetExclusive() : "offset " + walLogicalOffset + " " +
            "not writable in segment " + this;
        long position = positionForOffset(walLogicalOffset);
        ByteBuffer[] nioBuffers = src.nioBuffers();
        long bytesWritten = 0;
        for (ByteBuffer nioBuffer : nioBuffers) {
            while (nioBuffer.hasRemaining()) {
                int written = fileChannel.write(nioBuffer, position);
                if (written == -1) {
                    throw new IOException("write -1 at position " + position);
                }
                position += written;
                bytesWritten += written;
            }
        }
        src.readerIndex(src.writerIndex());
        long newEnd = positionForOffset(walLogicalOffset) + bytesWritten;
        return nextWritePosition.accumulateAndGet(newEnd, Math::max);
    }

    /**
     * Read {@code length} bytes starting at the given WAL logical offset, appending them to {@code dst}.
     */
    public int readAt(ByteBuf dst, long walLogicalOffset, int length) throws IOException {
        assert containsOffset(walLogicalOffset) : "offset " + walLogicalOffset + " not in segment " + this;
        long position = positionForOffset(walLogicalOffset);
        length = Math.min(length, dst.writableBytes());
        int total = 0;
        while (total < length) {
            int read = dst.writeBytes(fileChannel, position + total, length - total);
            if (read == -1) {
                break;
            }
            total += read;
        }
        return total;
    }

    /**
     * Force pending data to disk.
     */
    public void fsync() throws IOException {
        if (fileChannel == null) {
            throw new IOException("segment is closed: " + this);
        }
        fileChannel.force(false);
    }

    /**
     * Suggest the kernel evict page cache pages for the given WAL logical offset range. Best-effort,
     * currently a no-op pending native-fd integration.
     */
    public void adviseDontNeedRange(long walLogicalOffset, long length) {
    }

    public boolean containsOffset(long walLogicalOffset) {
        return walLogicalOffset >= startOffset && walLogicalOffset < endOffsetExclusive();
    }

    public long positionForOffset(long walLogicalOffset) {
        return walLogicalOffset - startOffset;
    }

    @Override
    public String toString() {
        return "Segment{startOffset=" + startOffset + ", endOffsetExclusive=" + endOffsetExclusive() + ", file=" + file + '}';
    }
}
