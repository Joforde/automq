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
import com.automq.stream.s3.Config;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.trace.TraceUtils;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.common.AppendResultImpl;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.common.ShutdownType;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.RuntimeIOException;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static com.automq.stream.s3.Constants.NOOP_EPOCH;
import static com.automq.stream.s3.Constants.NOOP_NODE_ID;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_MAGIC_CODE;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_WITHOUT_CRC_SIZE;

/**
 * A file-system based WAL implementation that stores the log across multiple rolling segment
 * files. When the active segment fills up to its configured cap the service rolls over to a new
 * segment; trimming advances a global trim offset and any segment that falls entirely below the
 * offset is deleted so that disk space is reclaimed.
 * <p>
 * Layout per segment file:
 * <pre>
 * [Header slot A: 4KiB][Header slot B: 4KiB][Records ...]
 * </pre>
 * Each segment owns a contiguous range of global record offsets, computed from its index and the
 * configured per-segment record byte capacity. The two header slots are written in round-robin so
 * at least one valid header survives a crash.
 * <p>
 * Append flow:
 * <ol>
 *   <li>{@link #append} accumulates records into a {@link Block}, which is handed off to the
 *   sliding window service.</li>
 *   <li>An IO thread writes the block payload to its segment <strong>without</strong> calling
 *   fsync.</li>
 *   <li>A single flush thread coalesces batches, fsyncs the affected segments once, completes
 *   the pending {@link AppendResult} futures and advises the kernel to drop the corresponding
 *   page cache range.</li>
 * </ol>
 */
public class FileSystemWALService implements WriteAheadLog {
    public static final int WAL_HEADER_COUNT = 2;
    public static final int WAL_HEADER_CAPACITY = WALUtil.BLOCK_SIZE;
    public static final int WAL_HEADER_TOTAL_CAPACITY = WAL_HEADER_CAPACITY * WAL_HEADER_COUNT;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemWALService.class);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean resetFinished = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final ExecutorService walHeaderFlusher = Threads.newFixedThreadPool(1,
        ThreadUtils.createThreadFactory("flush-file-wal-header-thread-%d", true), LOGGER);

    private File directory;
    private long maxSegmentSize;
    private long maxRetainedRecordBytes;
    private long initialWindowSize;
    private int timeoutSeconds;
    private boolean recoveryMode;
    private boolean firstStart;
    private int nodeId = NOOP_NODE_ID;
    private long epoch = NOOP_EPOCH;

    private SegmentManager segmentManager;
    private SlidingWindowService slidingWindowService;
    private FileSystemWALHeader walHeader;

    private FileSystemWALService() {
    }

    public static FileSystemWALServiceBuilder builder(String path, long maxSegmentSize, long maxRetainedRecordBytes) {
        return new FileSystemWALServiceBuilder(path, maxSegmentSize, maxRetainedRecordBytes);
    }

    public static FileSystemWALServiceBuilder builder(IdURI uri) {
        long maxSegmentSize = uri.extensionLong("segmentSize", 256L << 20);
        long maxRetainedRecordBytes = uri.extensionLong("capacity", 2147483648L);
        FileSystemWALServiceBuilder builder = new FileSystemWALServiceBuilder(uri.path(), maxSegmentSize, maxRetainedRecordBytes);
        Optional.ofNullable(uri.extensionString("iops")).filter(StringUtils::isNumeric).ifPresent(v -> builder.writeRateLimit(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("iodepth")).filter(StringUtils::isNumeric).ifPresent(v -> builder.ioThreadNums(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("iobandwidth")).filter(StringUtils::isNumeric).ifPresent(v -> builder.writeBandwidthLimit(Long.parseLong(v)));
        Optional.ofNullable(uri.extensionString("timeout")).filter(StringUtils::isNumeric).ifPresent(v -> builder.timeoutSeconds(Integer.parseInt(v)));
        return builder;
    }

    public static FileSystemWALServiceBuilder recoveryBuilder(String path) {
        return new FileSystemWALServiceBuilder(path).recoveryMode(true);
    }

    private void flushWALHeader(ShutdownType shutdownType) throws IOException {
        walHeader.setShutdownType(shutdownType);
        flushWALHeader();
    }

    /**
     * Flush the in-memory header into the latest segment so that the trim offset and other
     * metadata become durable. The latest segment is the one with the highest start offset.
     */
    private synchronized void flushWALHeader() throws IOException {
        Segment latest = segmentManager.latestSegment();
        if (latest == null) {
            // No segment has been created yet, nothing to persist.
            return;
        }
        latest.writeHeader(walHeader);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("WAL header flushed to segment {}: {}", latest, walHeader);
        }
    }

    /**
     * Try to read a record starting at the given global offset.
     */
    private ByteBuf readRecord(long recoverStartOffset) throws IOException, ReadRecordException {
        final ByteBuf recordHeader = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        RecordHeader readRecordHeader;
        try {
            readRecordHeader = parseRecordHeader(recoverStartOffset, recordHeader);
        } finally {
            recordHeader.release();
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        ByteBuf recordBody = ByteBufAlloc.byteBuffer(recordBodyLength);
        try {
            parseRecordBody(recoverStartOffset, readRecordHeader, recordBody);
        } catch (Exception e) {
            recordBody.release();
            throw e;
        }

        return recordBody;
    }

    private RecordHeader parseRecordHeader(long recoverStartOffset, ByteBuf recordHeader) throws IOException, ReadRecordException {
        Segment segment = segmentManager.segmentForOffset(recoverStartOffset);
        if (segment == null) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("no segment for offset %d", recoverStartOffset));
        }
        int read = segment.readAt(recordHeader, recoverStartOffset, RECORD_HEADER_SIZE);
        if (read != RECORD_HEADER_SIZE) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("failed to read record header: expected %d bytes, actual %d bytes, recoverStartOffset: %d", RECORD_HEADER_SIZE, read, recoverStartOffset));
        }

        RecordHeader readRecordHeader = RecordHeader.unmarshal(recordHeader);
        if (readRecordHeader.getMagicCode() != RECORD_HEADER_MAGIC_CODE) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("magic code mismatch: expected %d, actual %d, recoverStartOffset: %d", RECORD_HEADER_MAGIC_CODE, readRecordHeader.getMagicCode(), recoverStartOffset));
        }

        int recordHeaderCRC = readRecordHeader.getRecordHeaderCRC();
        int calculatedRecordHeaderCRC = WALUtil.crc32(recordHeader, RECORD_HEADER_WITHOUT_CRC_SIZE);
        if (recordHeaderCRC != calculatedRecordHeaderCRC) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("record header crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordHeaderCRC, recordHeaderCRC, recoverStartOffset));
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        if (recordBodyLength <= 0) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("invalid record body length: %d, recoverStartOffset: %d", recordBodyLength, recoverStartOffset));
        }

        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        if (recordBodyOffset != recoverStartOffset + RECORD_HEADER_SIZE) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("invalid record body offset: expected %d, actual %d, recoverStartOffset: %d", recoverStartOffset + RECORD_HEADER_SIZE, recordBodyOffset, recoverStartOffset));
        }
        return readRecordHeader;
    }

    private void parseRecordBody(long recoverStartOffset, RecordHeader readRecordHeader, ByteBuf recordBody) throws IOException, ReadRecordException {
        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        Segment segment = segmentManager.segmentForOffset(recordBodyOffset);
        if (segment == null) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                String.format("no segment for record body offset %d", recordBodyOffset));
        }
        int read = segment.readAt(recordBody, recordBodyOffset, recordBodyLength);
        if (read != recordBodyLength) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                String.format("failed to read record body: expected %d bytes, actual %d bytes, recoverStartOffset: %d", recordBodyLength, read, recoverStartOffset));
        }

        int recordBodyCRC = readRecordHeader.getRecordBodyCRC();
        int calculatedRecordBodyCRC = WALUtil.crc32(recordBody);
        if (recordBodyCRC != calculatedRecordBodyCRC) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                String.format("record body crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordBodyCRC, recordBodyCRC, recoverStartOffset));
        }
    }

    @Override
    public WriteAheadLog start() throws IOException {
        if (started.get()) {
            LOGGER.warn("file system WAL service already started");
            return this;
        }
        StopWatch stopWatch = StopWatch.createStarted();

        // Seed every newly created segment with the current in-memory header so a crash before
        // the next trim or window-scale event still leaves us with valid metadata on disk.
        segmentManager.setInitializer(segment -> {
            if (walHeader != null) {
                segment.writeHeader(walHeader);
            }
        });
        segmentManager.load();

        FileSystemWALHeader header = tryReadLatestHeader();
        if (null == header) {
            if (recoveryMode) {
                throw new IllegalStateException("failed to read WAL header in recovery mode");
            }
            header = newWALHeader(0);
            firstStart = true;
            LOGGER.info("no available WAL header, created a new one: {}", header);
        } else {
            LOGGER.info("read WAL header: {}", header);
        }

        header.setShutdownType(ShutdownType.UNGRACEFULLY);
        walHeaderReady(header);

        started.set(true);
        LOGGER.info("file system WAL service started, cost: {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
        return this;
    }

    private FileSystemWALHeader newWALHeader(long fileStartOffset) {
        return new FileSystemWALHeader(fileStartOffset, maxSegmentSize, initialWindowSize);
    }

    /**
     * Scan every segment file and return the header with the latest write timestamp.
     */
    private FileSystemWALHeader tryReadLatestHeader() throws IOException {
        FileSystemWALHeader latest = null;
        for (Segment segment : segmentManager.snapshot()) {
            FileSystemWALHeader header = segment.readLatestHeader();
            if (header == null) {
                continue;
            }
            if (latest == null || latest.getLastWriteTimestamp() < header.getLastWriteTimestamp()) {
                latest = header;
            }
        }
        return latest;
    }

    private void walHeaderReady(FileSystemWALHeader header) throws IOException {
        if (nodeId != NOOP_NODE_ID) {
            header.setNodeId(nodeId);
            header.setEpoch(epoch);
        }
        this.walHeader = header;
        if (!segmentManager.isEmpty()) {
            flushWALHeader();
        }
    }

    private void registerMetrics() {
        S3StreamMetricsManager.registerDeltaWalOffsetSupplier(() -> {
            try {
                return this.getCurrentStartOffset();
            } catch (Exception e) {
                LOGGER.error("failed to get current start offset", e);
                return 0L;
            }
        }, () -> walHeader.getFlushedTrimOffset());
    }

    private long getCurrentStartOffset() {
        Lock lock = slidingWindowService.getBlockLock();
        lock.lock();
        try {
            Block block = slidingWindowService.getCurrentBlockLocked();
            return block.startOffset() + block.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdownGracefully() {
        StopWatch stopWatch = StopWatch.createStarted();

        if (!started.getAndSet(false)) {
            LOGGER.warn("file system WAL service already shutdown or not started yet");
            return;
        }
        walHeaderFlusher.shutdown();
        try {
            if (!walHeaderFlusher.awaitTermination(5, TimeUnit.SECONDS)) {
                walHeaderFlusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            walHeaderFlusher.shutdownNow();
        }

        boolean gracefulShutdown = Optional.ofNullable(slidingWindowService)
            .map(s -> s.shutdown(1, TimeUnit.DAYS))
            .orElse(true);
        try {
            flushWALHeader(gracefulShutdown ? ShutdownType.GRACEFULLY : ShutdownType.UNGRACEFULLY);
        } catch (IOException ignored) {
            // shutdown anyway
        }

        segmentManager.close();

        LOGGER.info("file system WAL service shutdown gracefully: {}, cost: {} ms", gracefulShutdown, stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public WALMetadata metadata() {
        checkStarted();
        return new WALMetadata(walHeader.getNodeId(), walHeader.getEpoch());
    }

    @Override
    public AppendResult append(TraceContext context, ByteBuf buf, int crc) throws OverCapacityException {
        TraceContext.Scope scope = TraceUtils.createAndStartSpan(context, "FileSystemWALService::append");
        final long startTime = System.nanoTime();
        try {
            AppendResult result = append0(buf, crc);
            result.future().whenComplete((nil, ex) -> TraceUtils.endSpan(scope, ex));
            return result;
        } catch (Throwable t) {
            if (t instanceof OverCapacityException) {
                StorageOperationStats.getInstance().appendWALFullStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
            }
            buf.release();
            TraceUtils.endSpan(scope, t);
            throw t;
        }
    }

    private AppendResult append0(ByteBuf body, int crc) throws OverCapacityException {
        final long startTime = System.nanoTime();
        checkStarted();
        checkNotFailed();
        checkWriteMode();
        checkResetFinished();

        final long recordSize = RECORD_HEADER_SIZE + body.readableBytes();
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        long expectedWriteOffset;

        Lock lock = slidingWindowService.getBlockLock();
        lock.lock();
        try {
            Block block = slidingWindowService.getCurrentBlockLocked();
            Block.RecordSupplier recordSupplier = (offset, header) -> WALUtil.generateRecord(body, header, crc, offset);
            expectedWriteOffset = block.addRecord(recordSize, recordSupplier, appendResultFuture);
            if (expectedWriteOffset < 0) {
                block = slidingWindowService.sealAndNewBlockLocked(block, recordSize, walHeader.getFlushedTrimOffset(), maxRetainedRecordBytes);
                expectedWriteOffset = block.addRecord(recordSize, recordSupplier, appendResultFuture);
            }
        } finally {
            lock.unlock();
        }
        slidingWindowService.tryWriteBlock();

        CompletableFuture<AppendResult.CallbackResult> timedFuture = FutureUtil.timeoutWithNewReturn(appendResultFuture, timeoutSeconds, TimeUnit.SECONDS, () -> failed.set(true));
        final AppendResult appendResult = new AppendResultImpl(expectedWriteOffset, timedFuture);
        appendResult.future().whenComplete((nil, ex) -> StorageOperationStats.getInstance().appendWALCompleteStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS)));
        StorageOperationStats.getInstance().appendWALBeforeStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
        return appendResult;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        checkStarted();
        if (firstStart) {
            return Collections.emptyIterator();
        }

        long trimmedOffset = walHeader.getTrimOffset();
        long recoverStartOffset = Math.max(trimmedOffset, 0);
        long windowLength = walHeader.getSlidingWindowMaxLength();
        return new RecoverIterator(recoverStartOffset, windowLength, trimmedOffset);
    }

    @Override
    public CompletableFuture<Void> reset() {
        checkStarted();

        // Pick a fresh start offset aligned to the next segment boundary so we begin writing into
        // a clean segment after the existing data.
        long current = walHeader.getTrimOffset();
        long maxRecordBytesPerSegment = segmentManager.maxRecordBytesPerSegment();
        long base = Math.max(current, segmentManager.highestKnownEndOffset() - 1);
        long newStartOffset = ((base / maxRecordBytesPerSegment) + 1) * maxRecordBytesPerSegment;

        if (!recoveryMode) {
            slidingWindowService.start(walHeader.getAtomicSlidingWindowMaxLength(), newStartOffset);
        }
        LOGGER.info("reset sliding window to offset: {}", newStartOffset);
        CompletableFuture<Void> cf = trim(newStartOffset - 1, true)
            .thenRun(() -> resetFinished.set(true));

        if (!recoveryMode) {
            return cf.thenRun(this::registerMetrics);
        }
        return cf;
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        return trim(offset, false);
    }

    private CompletableFuture<Void> trim(long offset, boolean internal) {
        checkStarted();
        if (!internal) {
            checkWriteMode();
            checkResetFinished();
            if (offset >= slidingWindowService.getWindowCoreData().getStartOffset()) {
                throw new IllegalArgumentException("failed to trim: record at offset " + offset + " has not been flushed yet");
            }
        }

        walHeader.updateTrimOffset(offset);
        return CompletableFuture.runAsync(() -> {
            try {
                flushWALHeader();
                // After the trim offset is durable, segments fully covered by it can be removed.
                int deleted = segmentManager.deleteSegmentsBelowOrEqual(walHeader.getFlushedTrimOffset());
                if (deleted > 0) {
                    LOGGER.info("removed {} fully-trimmed segment file(s)", deleted);
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }, walHeaderFlusher);
    }

    private void checkStarted() {
        if (!started.get()) {
            throw new IllegalStateException("WriteAheadLog has not been started yet");
        }
    }

    private void checkNotFailed() {
        if (failed.get()) {
            throw new RuntimeIOException(new IOException("WriteAheadLog is in failed state"));
        }
    }

    private void checkWriteMode() {
        if (recoveryMode) {
            throw new IllegalStateException("WriteAheadLog is in recovery mode");
        }
    }

    private void checkResetFinished() {
        if (!resetFinished.get()) {
            throw new IllegalStateException("WriteAheadLog has not been reset yet");
        }
    }

    private SlidingWindowService.WALHeaderFlusher flusher() {
        return () -> flushWALHeader(ShutdownType.UNGRACEFULLY);
    }

    public static class FileSystemWALServiceBuilder {
        private final String directoryPath;
        private long maxSegmentSize = 256L << 20; // 256MiB
        private long maxRetainedRecordBytes = 2147483648L; // 2GiB
        private int initBufferSize = 1 << 20;
        private int maxBufferSize = 1 << 27;
        private int ioThreadNums = 8;
        private long slidingWindowInitialSize = 1 << 20;
        private long slidingWindowUpperLimit = 1 << 29;
        private long slidingWindowScaleUnit = 1 << 22;
        private long blockSoftLimit = 1 << 18;
        private int writeRateLimit = 3000;
        private long writeBandwidthLimit = Long.MAX_VALUE;
        private int timeoutSeconds = 30;
        private int nodeId = NOOP_NODE_ID;
        private long epoch = NOOP_EPOCH;
        private boolean recoveryMode = false;

        public FileSystemWALServiceBuilder(String directoryPath) {
            this.directoryPath = directoryPath;
        }

        public FileSystemWALServiceBuilder(String directoryPath, long maxSegmentSize, long maxRetainedRecordBytes) {
            this.directoryPath = directoryPath;
            this.maxSegmentSize = maxSegmentSize;
            this.maxRetainedRecordBytes = maxRetainedRecordBytes;
        }

        public FileSystemWALServiceBuilder recoveryMode(boolean recoveryMode) {
            this.recoveryMode = recoveryMode;
            return this;
        }

        public FileSystemWALServiceBuilder maxSegmentSize(long maxSegmentSize) {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }

        public FileSystemWALServiceBuilder maxRetainedRecordBytes(long maxRetainedRecordBytes) {
            this.maxRetainedRecordBytes = maxRetainedRecordBytes;
            return this;
        }

        public FileSystemWALServiceBuilder config(Config config) {
            return this
                .nodeId(config.nodeId())
                .epoch(config.nodeEpoch());
        }

        public FileSystemWALServiceBuilder initBufferSize(int initBufferSize) {
            this.initBufferSize = initBufferSize;
            return this;
        }

        public FileSystemWALServiceBuilder maxBufferSize(int maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public FileSystemWALServiceBuilder ioThreadNums(int ioThreadNums) {
            this.ioThreadNums = ioThreadNums;
            return this;
        }

        public FileSystemWALServiceBuilder slidingWindowInitialSize(long slidingWindowInitialSize) {
            this.slidingWindowInitialSize = slidingWindowInitialSize;
            return this;
        }

        public FileSystemWALServiceBuilder slidingWindowUpperLimit(long slidingWindowUpperLimit) {
            this.slidingWindowUpperLimit = slidingWindowUpperLimit;
            return this;
        }

        public FileSystemWALServiceBuilder slidingWindowScaleUnit(long slidingWindowScaleUnit) {
            this.slidingWindowScaleUnit = slidingWindowScaleUnit;
            return this;
        }

        public FileSystemWALServiceBuilder blockSoftLimit(long blockSoftLimit) {
            this.blockSoftLimit = blockSoftLimit;
            return this;
        }

        public FileSystemWALServiceBuilder writeRateLimit(int writeRateLimit) {
            this.writeRateLimit = writeRateLimit;
            return this;
        }

        public FileSystemWALServiceBuilder writeBandwidthLimit(long writeBandwidthLimit) {
            this.writeBandwidthLimit = writeBandwidthLimit;
            return this;
        }

        public FileSystemWALServiceBuilder timeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            return this;
        }

        public FileSystemWALServiceBuilder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public FileSystemWALServiceBuilder epoch(long epoch) {
            this.epoch = epoch;
            return this;
        }

        public FileSystemWALService build() {
            if (recoveryMode) {
                if (nodeId != NOOP_NODE_ID) {
                    LOGGER.warn("node id should not be set in recovery mode, but got {}, ignore it", nodeId);
                    nodeId = NOOP_NODE_ID;
                }
                if (epoch != NOOP_EPOCH) {
                    LOGGER.warn("node epoch should not be set in recovery mode, but got {}, ignore it", epoch);
                    epoch = NOOP_EPOCH;
                }
            }

            // Align the per-segment file size to the block size so that headers and records remain
            // block aligned on disk.
            maxSegmentSize = WALUtil.alignLargeByBlockSize(maxSegmentSize);
            if (maxSegmentSize <= WAL_HEADER_TOTAL_CAPACITY) {
                throw new IllegalArgumentException("maxSegmentSize must be larger than " + WAL_HEADER_TOTAL_CAPACITY);
            }

            FileSystemWALService service = new FileSystemWALService();
            service.directory = new File(directoryPath);
            service.maxSegmentSize = maxSegmentSize;
            service.maxRetainedRecordBytes = maxRetainedRecordBytes;
            service.segmentManager = new SegmentManager(service.directory, maxSegmentSize);
            service.timeoutSeconds = timeoutSeconds;
            service.recoveryMode = recoveryMode;
            if (nodeId != NOOP_NODE_ID) {
                service.nodeId = nodeId;
                service.epoch = epoch;
            }

            long maxRecordBytesPerSegment = maxSegmentSize - WAL_HEADER_TOTAL_CAPACITY;
            if (!recoveryMode) {
                slidingWindowInitialSize = Math.min(slidingWindowInitialSize, maxRecordBytesPerSegment);
                slidingWindowUpperLimit = Math.min(slidingWindowUpperLimit, maxRecordBytesPerSegment);
                service.initialWindowSize = slidingWindowInitialSize;
                service.slidingWindowService = new SlidingWindowService(
                    service.segmentManager,
                    ioThreadNums,
                    slidingWindowUpperLimit,
                    slidingWindowScaleUnit,
                    blockSoftLimit,
                    Math.max(writeRateLimit - 20, writeRateLimit / 2),
                    writeBandwidthLimit,
                    service.flusher()
                );
            }

            LOGGER.info("build FileSystemWALService: {}", this);
            return service;
        }

        @Override
        public String toString() {
            return "FileSystemWALServiceBuilder{"
                + "directoryPath='" + directoryPath
                + ", maxSegmentSize=" + maxSegmentSize
                + ", maxRetainedRecordBytes=" + maxRetainedRecordBytes
                + ", initBufferSize=" + initBufferSize
                + ", maxBufferSize=" + maxBufferSize
                + ", ioThreadNums=" + ioThreadNums
                + ", slidingWindowInitialSize=" + slidingWindowInitialSize
                + ", slidingWindowUpperLimit=" + slidingWindowUpperLimit
                + ", slidingWindowScaleUnit=" + slidingWindowScaleUnit
                + ", blockSoftLimit=" + blockSoftLimit
                + ", writeRateLimit=" + writeRateLimit
                + ", writeBandwidthLimit=" + writeBandwidthLimit
                + ", timeoutSeconds=" + timeoutSeconds
                + ", nodeId=" + nodeId
                + ", epoch=" + epoch
                + ", recoveryMode=" + recoveryMode
                + '}';
        }
    }

    static class ReadRecordException extends Exception {
        final long jumpNextRecoverOffset;

        ReadRecordException(long offset, String message) {
            super(message);
            this.jumpNextRecoverOffset = offset;
        }

        long getJumpNextRecoverOffset() {
            return jumpNextRecoverOffset;
        }
    }

    /**
     * Linear scan recovery: walks forward from the first un-trimmed offset, decoding records and
     * jumping over corrupted ones.
     */
    protected class RecoverIterator implements Iterator<RecoverResult> {
        private final long windowLength;
        private final long skipRecordAtOffset;
        private long nextRecoverOffset;
        private long lastValidOffset = -1;
        private boolean firstInvalidSeen;
        private long firstInvalidOffset = -1;
        private RecoverResult next;

        public RecoverIterator(long nextRecoverOffset, long windowLength, long skipRecordAtOffset) {
            this.nextRecoverOffset = nextRecoverOffset;
            this.skipRecordAtOffset = skipRecordAtOffset;
            this.windowLength = windowLength;
        }

        @Override
        public boolean hasNext() throws RuntimeIOException {
            return tryReadNextRecord();
        }

        @Override
        public RecoverResult next() throws RuntimeIOException {
            if (!tryReadNextRecord()) {
                throw new NoSuchElementException();
            }
            RecoverResult result = next;
            this.next = null;
            return result;
        }

        private boolean tryReadNextRecord() throws RuntimeIOException {
            if (next != null) {
                return true;
            }
            while (shouldContinue()) {
                Segment segment = segmentManager.segmentForOffset(nextRecoverOffset);
                if (segment == null) {
                    // The segment that should contain this offset has been trimmed away. Jump to
                    // the next available segment to keep recovering.
                    Segment nextSegment = segmentManager.nextSegmentAfter(nextRecoverOffset);
                    if (nextSegment == null) {
                        return false;
                    }
                    nextRecoverOffset = nextSegment.startOffset();
                    continue;
                }
                boolean skip = nextRecoverOffset == skipRecordAtOffset;
                try {
                    ByteBuf nextRecordBody = readRecord(nextRecoverOffset);
                    RecoverResultImpl recoverResult = new RecoverResultImpl(nextRecordBody, nextRecoverOffset);
                    lastValidOffset = nextRecoverOffset;
                    nextRecoverOffset += RECORD_HEADER_SIZE + nextRecordBody.readableBytes();
                    if (skip) {
                        nextRecordBody.release();
                        continue;
                    }
                    next = recoverResult;
                    return true;
                } catch (ReadRecordException e) {
                    if (!firstInvalidSeen && WALUtil.isAligned(nextRecoverOffset) && !skip) {
                        firstInvalidSeen = true;
                        firstInvalidOffset = nextRecoverOffset;
                        LOGGER.info("first invalid offset met during recovery, offset {}, detail: '{}'",
                            firstInvalidOffset, e.getMessage());
                    }
                    nextRecoverOffset = e.getJumpNextRecoverOffset();
                } catch (IOException e) {
                    LOGGER.error("failed to read record at offset {}", nextRecoverOffset, e);
                    throw new RuntimeIOException(e);
                }
            }
            return false;
        }

        private boolean shouldContinue() {
            if (!firstInvalidSeen) {
                return segmentManager.segmentForOffset(nextRecoverOffset) != null
                    || segmentManager.nextSegmentAfter(nextRecoverOffset) != null;
            }
            // After the first invalid record we still look a little further to tolerate scattered
            // bad records, but stop once we have walked past the window.
            long slack = Math.min(windowLength, 1 << 22);
            return nextRecoverOffset < (lastValidOffset >= 0 ? lastValidOffset : firstInvalidOffset) + slack;
        }
    }

    // Accessor used by tests.
    SegmentManager segmentManager() {
        return segmentManager;
    }

    AtomicLong slidingWindowMaxLength() {
        return walHeader.getAtomicSlidingWindowMaxLength();
    }
}
