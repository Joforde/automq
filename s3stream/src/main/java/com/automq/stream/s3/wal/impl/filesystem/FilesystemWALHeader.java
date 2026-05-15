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
 * Byte layout (fields written in this order):
 * <pre>
 * [4B]  magicCode          - identifies a valid header slot
 * [8B]  fileStartOffset    - global logical offset of the first record byte in this segment
 * [8B]  trimOffset         - global trim offset; records before this are no longer needed
 * [8B]  lastWriteTimestamp - System.nanoTime() at write time; used to select the newest slot on recovery
 * [4B]  shutdownType       - graceful vs. ungraceful last shutdown
 * [4B]  nodeId             - owning broker node id
 * [8B]  epoch              - owning broker epoch
 * [4B]  crc                - CRC-32 over all preceding bytes
 * </pre>
 */
public class FilesystemWALHeader {
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345679;
    public static final int WAL_HEADER_SIZE = 4 // magic code
        + 8 // file start offset
        + 8 // trim offset
        + 8 // last write timestamp
        + 4 // shutdown type
        + 4 // node id
        + 8 // node epoch
        + 4; // crc
    public static final int WAL_HEADER_WITHOUT_CRC_SIZE = WAL_HEADER_SIZE - 4;

    private final AtomicLong trimOffset = new AtomicLong(-1);
    private final AtomicLong flushedTrimOffset = new AtomicLong(-1);
    private int magicCode = WAL_HEADER_MAGIC_CODE;
    private long fileStartOffset;
    private long lastWriteTimestamp = System.nanoTime();
    private ShutdownType shutdownType = ShutdownType.UNGRACEFULLY;
    private int nodeId;
    private long epoch;
    private int crc;

    public FilesystemWALHeader(long fileStartOffset) {
        this.fileStartOffset = fileStartOffset;
    }

    public static FilesystemWALHeader unmarshal(ByteBuf buf) throws UnmarshalException {
        FilesystemWALHeader header = new FilesystemWALHeader(0);
        buf.markReaderIndex();
        header.magicCode = buf.readInt();
        header.fileStartOffset = buf.readLong();
        long trimOffset = buf.readLong();
        header.trimOffset.set(trimOffset);
        header.flushedTrimOffset.set(trimOffset);
        header.lastWriteTimestamp = buf.readLong();
        header.shutdownType = ShutdownType.fromCode(buf.readInt());
        header.nodeId = buf.readInt();
        header.epoch = buf.readLong();
        header.crc = buf.readInt();
        buf.resetReaderIndex();

        if (header.magicCode != WAL_HEADER_MAGIC_CODE) {
            throw new UnmarshalException(String.format("Filesystem WAL header magic code mismatch, recovered: [%d] expect: [%d]", header.magicCode, WAL_HEADER_MAGIC_CODE));
        }

        int computedCrc = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        if (computedCrc != header.crc) {
            throw new UnmarshalException(String.format("Filesystem WAL header CRC mismatch, recovered: [%d] computed: [%d]", header.crc, computedCrc));
        }

        return header;
    }

    public long getFileStartOffset() {
        return fileStartOffset;
    }

    public long getTrimOffset() {
        return trimOffset.get();
    }

    public FilesystemWALHeader updateTrimOffset(long offset) {
        trimOffset.accumulateAndGet(offset, Math::max);
        return this;
    }

    public long getFlushedTrimOffset() {
        return flushedTrimOffset.get();
    }

    public void updateFlushedTrimOffset(long offset) {
        flushedTrimOffset.accumulateAndGet(offset, Math::max);
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp;
    }

    public FilesystemWALHeader setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp = lastWriteTimestamp;
        return this;
    }

    public ShutdownType getShutdownType() {
        return shutdownType;
    }

    public FilesystemWALHeader setShutdownType(ShutdownType shutdownType) {
        this.shutdownType = shutdownType;
        return this;
    }

    public int getNodeId() {
        return nodeId;
    }

    public FilesystemWALHeader setNodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public long getEpoch() {
        return epoch;
    }

    public FilesystemWALHeader setEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    @Override
    public String toString() {
        return "FilesystemWALHeader{"
            + "magicCode=" + magicCode
            + ", fileStartOffset=" + fileStartOffset
            + ", trimOffset=" + trimOffset
            + ", lastWriteTimestamp=" + lastWriteTimestamp
            + ", shutdownType=" + shutdownType
            + ", nodeId=" + nodeId
            + ", epoch=" + epoch
            + ", crc=" + crc
            + '}';
    }

    private ByteBuf marshalHeaderExceptCRC() {
        ByteBuf buf = ByteBufAlloc.byteBuffer(WAL_HEADER_SIZE);
        buf.writeInt(magicCode);
        buf.writeLong(fileStartOffset);
        buf.writeLong(trimOffset.get());
        buf.writeLong(lastWriteTimestamp);
        buf.writeInt(shutdownType.getCode());
        buf.writeInt(nodeId);
        buf.writeLong(epoch);
        return buf;
    }

    public ByteBuf marshal() {
        ByteBuf buf = marshalHeaderExceptCRC();
        this.crc = WALUtil.crc32(buf, WAL_HEADER_WITHOUT_CRC_SIZE);
        buf.writeInt(crc);
        return buf;
    }
}
