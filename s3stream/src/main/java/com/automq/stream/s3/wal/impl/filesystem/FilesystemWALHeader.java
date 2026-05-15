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
import com.automq.stream.s3.wal.common.ShutdownType;
import com.automq.stream.s3.wal.exception.UnmarshalException;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Layout of a segment file header, persisted at the very beginning of every WAL segment file.
 * <p>
 * Field index - size - description:
 * <p>
 * 0 - [4B] {@link FileSystemWALHeader#magicCode0} Magic code used to verify the start of the header
 * <p>
 * 1 - [8B] {@link FileSystemWALHeader#fileStartOffset1} The global logical offset of the first record byte in this segment
 * <p>
 * 2 - [8B] {@link FileSystemWALHeader#maxSegmentSize2} The configured max bytes of this segment file
 * <p>
 * 3 - [8B] {@link FileSystemWALHeader#trimOffset3} The global trim offset. Records strictly before this offset are considered useless
 * <p>
 * 4 - [8B] {@link FileSystemWALHeader#lastWriteTimestamp4} Timestamp of the last header write, used to pick the newest header on recovery
 * <p>
 * 5 - [8B] {@link FileSystemWALHeader#slidingWindowMaxLength5} The current sliding window max length
 * <p>
 * 6 - [4B] {@link FileSystemWALHeader#shutdownType6} The shutdown type when the header was last written
 * <p>
 * 7 - [4B] {@link FileSystemWALHeader#nodeId7} the node id
 * <p>
 * 8 - [8B] {@link FileSystemWALHeader#epoch8} the node epoch
 * <p>
 * 9 - [4B] {@link FileSystemWALHeader#crc9} CRC of the rest of the header.
 */
public class FileSystemWALHeader {
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345679;
    public static final int WAL_HEADER_SIZE = 4 // magic code
        + 8 // file start offset
        + 8 // max segment size
        + 8 // trim offset
        + 8 // last write timestamp
        + 8 // sliding window max length
        + 4 // shutdown type
        + 4 // node id
        + 8 // node epoch
        + 4; // crc
    public static final int WAL_HEADER_WITHOUT_CRC_SIZE = WAL_HEADER_SIZE - 4;

    private final AtomicLong trimOffset3 = new AtomicLong(-1);
    private final AtomicLong flushedTrimOffset = new AtomicLong(-1);
    private final AtomicLong slidingWindowMaxLength5 = new AtomicLong(0);
    private int magicCode0 = WAL_HEADER_MAGIC_CODE;
    private long fileStartOffset1;
    private long maxSegmentSize2;
    private long lastWriteTimestamp4 = System.nanoTime();
    private ShutdownType shutdownType6 = ShutdownType.UNGRACEFULLY;
    private int nodeId7;
    private long epoch8;
    private int crc9;

    public FileSystemWALHeader(long fileStartOffset, long maxSegmentSize, long windowMaxLength) {
        this.fileStartOffset1 = fileStartOffset;
        this.maxSegmentSize2 = maxSegmentSize;
        this.slidingWindowMaxLength5.set(windowMaxLength);
    }

    public static FileSystemWALHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        FileSystemWALHeader header = new FileSystemWALHeader(0, 0, 0);
        buf.markReaderIndex();
        header.magicCode0 = buf.readInt();
        header.fileStartOffset1 = buf.readLong();
        header.maxSegmentSize2 = buf.readLong();
        long trimOffset = buf.readLong();
        header.trimOffset3.set(trimOffset);
        header.flushedTrimOffset.set(trimOffset);
        header.lastWriteTimestamp4 = buf.readLong();
        header.slidingWindowMaxLength5.set(buf.readLong());
        header.shutdownType6 = ShutdownType.fromCode(buf.readInt());
        header.nodeId7 = buf.readInt();
        header.epoch8 = buf.readLong();
        header.crc9 = buf.readInt();
        buf.resetReaderIndex();

        if (header.magicCode0 != WAL_HEADER_MAGIC_CODE) {
            throw new UnmarshalException(String.format("WALHeader MagicCode not match, Recovered: [%d] expect: [%d]", header.magicCode0, WAL_HEADER_MAGIC_CODE));
        }

        int crc = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        if (crc != header.crc9) {
            throw new UnmarshalException(String.format("WALHeader CRC not match, Recovered: [%d] expect: [%d]", header.crc9, crc));
        }

        return header;
    }

    public long getFileStartOffset() {
        return fileStartOffset1;
    }

    public long getMaxSegmentSize() {
        return maxSegmentSize2;
    }

    public long getTrimOffset() {
        return trimOffset3.get();
    }

    public FileSystemWALHeader updateTrimOffset(long trimOffset) {
        trimOffset3.accumulateAndGet(trimOffset, Math::max);
        return this;
    }

    public long getFlushedTrimOffset() {
        return flushedTrimOffset.get();
    }

    public void updateFlushedTrimOffset(long flushedTrimOffset) {
        this.flushedTrimOffset.accumulateAndGet(flushedTrimOffset, Math::max);
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp4;
    }

    public FileSystemWALHeader setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp4 = lastWriteTimestamp;
        return this;
    }

    public long getSlidingWindowMaxLength() {
        return slidingWindowMaxLength5.get();
    }

    public AtomicLong getAtomicSlidingWindowMaxLength() {
        return slidingWindowMaxLength5;
    }

    public ShutdownType getShutdownType() {
        return shutdownType6;
    }

    public FileSystemWALHeader setShutdownType(ShutdownType shutdownType) {
        this.shutdownType6 = shutdownType;
        return this;
    }

    public int getNodeId() {
        return nodeId7;
    }

    public FileSystemWALHeader setNodeId(int nodeId) {
        this.nodeId7 = nodeId;
        return this;
    }

    public long getEpoch() {
        return epoch8;
    }

    public FileSystemWALHeader setEpoch(long epoch) {
        this.epoch8 = epoch;
        return this;
    }

    @Override
    public String toString() {
        return "FileSystemWALHeader{"
            + "magicCode=" + magicCode0
            + ", fileStartOffset=" + fileStartOffset1
            + ", maxSegmentSize=" + maxSegmentSize2
            + ", trimOffset=" + trimOffset3
            + ", lastWriteTimestamp=" + lastWriteTimestamp4
            + ", slidingWindowMaxLength=" + slidingWindowMaxLength5
            + ", shutdownType=" + shutdownType6
            + ", nodeId=" + nodeId7
            + ", epoch=" + epoch8
            + ", crc=" + crc9
            + '}';
    }

    private ByteBuf marshalHeaderExceptCRC() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(WAL_HEADER_SIZE);
        buf.writeInt(magicCode0);
        buf.writeLong(fileStartOffset1);
        buf.writeLong(maxSegmentSize2);
        buf.writeLong(trimOffset3.get());
        buf.writeLong(lastWriteTimestamp4);
        buf.writeLong(slidingWindowMaxLength5.get());
        buf.writeInt(shutdownType6.getCode());
        buf.writeInt(nodeId7);
        buf.writeLong(epoch8);
        return buf;
    }

    ByteBuf marshal() {
        ByteBuf buf = marshalHeaderExceptCRC();
        this.crc9 = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        buf.writeInt(crc9);
        return buf;
    }
}
