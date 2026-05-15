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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;

/**
 * A single WAL segment file.
 * <p>
 * Layout on disk:
 * <pre>
 * [Header slot A: {@link FileSystemWALService#WAL_HEADER_CAPACITY}]
 * [Header slot B: {@link FileSystemWALService#WAL_HEADER_CAPACITY}]
 * [Record bytes ...]
 * </pre>
 * Each segment owns a contiguous range of global record offsets
 * {@code [startOffset, startOffset + maxRecordBytes)}. The two header slots are written in
 * round-robin so that there is always at least one valid header available after a crash.
 */
class Segment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Segment.class);

    private final long segmentIndex;
    private final long startOffset;
    private final long maxSegmentSize;
    private final long maxRecordBytes;
    private final File file;

    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private int fd = -1;

    private final AtomicLong writeHeaderRoundTimes = new AtomicLong(0);

    Segment(File file, long segmentIndex, long maxSegmentSize) {
        this.file = file;
        this.segmentIndex = segmentIndex;
        this.maxSegmentSize = maxSegmentSize;
        this.maxRecordBytes = maxSegmentSize - FileSystemWALService.WAL_HEADER_TOTAL_CAPACITY;
        this.startOffset = segmentIndex * maxRecordBytes;
    }

    long segmentIndex() {
        return segmentIndex;
    }

    long startOffset() {
        return startOffset;
    }

    long maxRecordBytes() {
        return maxRecordBytes;
    }

    long endOffsetExclusive() {
        return startOffset + maxRecordBytes;
    }

    File file() {
        return file;
    }

    /**
     * Open (and create when necessary) the underlying file. Newly created files are pre-allocated to
     * {@link #maxSegmentSize} so that subsequent writes do not need to extend the file.
     */
    void openOrCreate() throws IOException {
        System.err.println("[DEBUG] openOrCreate: enter, file=" + file);
        boolean exists = file.exists();
        if (!exists) {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IOException("mkdirs " + parent + " failed");
            }
            System.err.println("[DEBUG] openOrCreate: before createNewFile");
            if (!file.createNewFile()) {
                throw new IOException("create " + file + " failed");
            }
            System.err.println("[DEBUG] openOrCreate: after createNewFile");
        }
        System.err.println("[DEBUG] openOrCreate: before new RandomAccessFile");
        raf = new RandomAccessFile(file, "rw");
        System.err.println("[DEBUG] openOrCreate: after new RandomAccessFile");
        if (!exists) {
            System.err.println("[DEBUG] openOrCreate: before setLength " + maxSegmentSize);
            raf.setLength(maxSegmentSize);
            System.err.println("[DEBUG] openOrCreate: after setLength");
        } else if (raf.length() != maxSegmentSize) {
            // Existing segment must match the configured size; otherwise the offset mapping is broken.
            throw new IOException(String.format("segment %s size mismatch, expected %d, actual %d",
                file, maxSegmentSize, raf.length()));
        }
        fileChannel = raf.getChannel();
        FileDescriptor descriptor = raf.getFD();
        System.err.println("[DEBUG] openOrCreate: before extractFd");
        fd = PageCacheAdvice.extractFd(descriptor);
        System.err.println("[DEBUG] openOrCreate: after extractFd, fd=" + fd);
    }

    void close() {
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
        fd = -1;
    }

    /**
     * Delete the underlying file. The segment is closed first.
     */
    void deleteQuietly() {
        close();
        if (file.exists() && !file.delete()) {
            LOGGER.warn("failed to delete segment file {}", file);
        }
    }

    /**
     * Write a record body at the given global offset. The caller must guarantee that the offset
     * falls within this segment.
     */
    void writeAt(ByteBuf src, long globalOffset) throws IOException {
        assert containsOffset(globalOffset) : "offset " + globalOffset + " not in segment " + this;
        long position = positionForOffset(globalOffset);
        ByteBuffer[] nioBuffers = src.nioBuffers();
        for (ByteBuffer nioBuffer : nioBuffers) {
            while (nioBuffer.hasRemaining()) {
                int written = fileChannel.write(nioBuffer, position);
                if (written == -1) {
                    throw new IOException("write -1 at position " + position);
                }
                position += written;
            }
        }
        src.readerIndex(src.writerIndex());
    }

    /**
     * Read {@code length} bytes starting at the given global offset, appending them to {@code dst}.
     */
    int readAt(ByteBuf dst, long globalOffset, int length) throws IOException {
        assert containsOffset(globalOffset) : "offset " + globalOffset + " not in segment " + this;
        long position = positionForOffset(globalOffset);
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
    void fsync() throws IOException {
        fileChannel.force(false);
    }

    /**
     * Read both header slots and return whichever one is newer (by lastWriteTimestamp). Returns
     * {@code null} when neither slot is valid.
     */
    FileSystemWALHeader readLatestHeader() throws IOException {
        FileSystemWALHeader latest = null;
        for (int i = 0; i < FileSystemWALService.WAL_HEADER_COUNT; i++) {
            ByteBuf buf = ByteBufAlloc.byteBuffer(FileSystemWALHeader.WAL_HEADER_SIZE);
            try {
                long position = (long) i * FileSystemWALService.WAL_HEADER_CAPACITY;
                int total = 0;
                while (total < FileSystemWALHeader.WAL_HEADER_SIZE) {
                    int read = buf.writeBytes(fileChannel, position + total, FileSystemWALHeader.WAL_HEADER_SIZE - total);
                    if (read == -1) {
                        break;
                    }
                    total += read;
                }
                if (total != FileSystemWALHeader.WAL_HEADER_SIZE) {
                    continue;
                }
                FileSystemWALHeader candidate = FileSystemWALHeader.unmarshal(buf);
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

    /**
     * Persist {@code header} to one of the two header slots (round-robin) and fsync. The header
     * field {@code lastWriteTimestamp} is updated by this call.
     */
    synchronized void writeHeader(FileSystemWALHeader header) throws IOException {
        System.err.println("[DEBUG] writeHeader: enter, segment=" + segmentIndex);
        long position = writeHeaderRoundTimes.getAndIncrement() % FileSystemWALService.WAL_HEADER_COUNT
            * FileSystemWALService.WAL_HEADER_CAPACITY;
        header.setLastWriteTimestamp(System.nanoTime());
        long trimOffset = header.getTrimOffset();
        System.err.println("[DEBUG] writeHeader: before marshal, segment=" + segmentIndex);
        ByteBuf buf = header.marshal();
        System.err.println("[DEBUG] writeHeader: after marshal, segment=" + segmentIndex);
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
            System.err.println("[DEBUG] writeHeader: before force, segment=" + segmentIndex);
            fileChannel.force(true);
            System.err.println("[DEBUG] writeHeader: after force, segment=" + segmentIndex);
        } finally {
            buf.release();
        }
        header.updateFlushedTrimOffset(trimOffset);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("WAL header flushed, segment {}, position {}, header {}", segmentIndex, position, header);
        }
    }

    /**
     * Suggest the kernel evict page cache pages for the given global offset range. Best-effort.
     */
    void adviseDontNeedRange(long globalOffset, long length) {
        if (fd < 0 || length <= 0) {
            return;
        }
        // Clip to this segment.
        long clipped = Math.min(length, endOffsetExclusive() - globalOffset);
        if (clipped <= 0) {
            return;
        }
        long position = positionForOffset(globalOffset);
        PageCacheAdvice.dontNeed(fd, position, clipped);
    }

    boolean containsOffset(long globalOffset) {
        return globalOffset >= startOffset && globalOffset < endOffsetExclusive();
    }

    long positionForOffset(long globalOffset) {
        return FileSystemWALService.WAL_HEADER_TOTAL_CAPACITY + (globalOffset - startOffset);
    }

    @Override
    public String toString() {
        return "Segment{index=" + segmentIndex
            + ", startOffset=" + startOffset
            + ", endOffsetExclusive=" + endOffsetExclusive()
            + ", file=" + file
            + '}';
    }

    /**
     * Standard segment file name encoding.
     */
    static String buildFileName(long segmentIndex) {
        return String.format("wal-%020d.log", segmentIndex);
    }

    /**
     * Parse a segment file name back to its index. Returns -1 if it does not match the pattern.
     */
    static long parseSegmentIndex(String fileName) {
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

    /**
     * Compute segment index for a global record offset, given the per-segment record byte capacity.
     */
    static long indexForOffset(long globalOffset, long maxRecordBytes) {
        return globalOffset / maxRecordBytes;
    }
}
