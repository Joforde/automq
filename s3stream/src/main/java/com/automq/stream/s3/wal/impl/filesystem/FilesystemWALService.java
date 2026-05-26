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

import com.automq.stream.FixedSizeByteBufPool;
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
import com.automq.stream.s3.wal.common.BatchedBlockingQueue;
import com.automq.stream.s3.wal.common.BlockingMpscQueue;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.common.ShutdownType;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.RuntimeIOException;
import com.automq.stream.s3.wal.exception.WALShutdownException;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
 * WAL directory layout:
 * <pre>
 * wal.meta                 - dedicated metadata file (trim offset, node id, shutdown type, ...)
 * wal-&lt;offset&gt;.log         - append-only data segments (record bytes only)
 * </pre>
 * <p>
 * Append flow:
 * <ol>
 *   <li>{@link #append} builds a {@link QueueEntry} and enqueues it for the write thread.</li>
 *   <li>The write thread writes each queued record to its segment <strong>without</strong> calling
 *   fsync.</li>
 *   <li>A single flush thread coalesces batches, fsyncs the affected segments once, completes
 *   the pending {@link AppendResult} futures and advises the kernel to drop the corresponding
 *   page cache range.</li>
 * </ol>
 */
public class FilesystemWALService implements WriteAheadLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemWALService.class);
    private static final FixedSizeByteBufPool HEADER_POOL = new FixedSizeByteBufPool(RECORD_HEADER_SIZE,
        1024 * Systems.CPU_CORES);
    /**
     * Capacity of the append work queue and of the queue feeding the fsync coalescer.
     */
    private static final int DEFAULT_APPEND_QUEUE_CAPACITY = 1000;
    /**
     * Max number of {@link ForceWriteRequest} items pulled per timed poll in {@link ForceWriteLoop}.
     */
    private static final int FSYNC_COALESCE_BUFFER_CAPACITY = 1000;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong flushedMarkOffset = new AtomicLong(0);
    private final AtomicBoolean resetFinished = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final ExecutorService walHeaderFlushExecutor = Threads.newFixedThreadPool(1,
        ThreadUtils.createThreadFactory("flush-file-wal-header-thread-%d", true), LOGGER);

    private final ExecutorService writeExecutor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory("file"
        + "-wal-write-thread-%d", true), LOGGER);

    private final ExecutorService forceWriteExecutor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
        "file-wal-force-write-thread-%d", true), LOGGER);

    private final BatchedBlockingQueue<QueueEntry> appendWorkQueue;
    private final BatchedBlockingQueue<ForceWriteRequest> pendingFsyncQueue;
    /**
     * Serializes assignment of {@link #nextAppendOffset} (next record's logical start offset).
     */
    private final ReentrantLock appendOffsetLock = new ReentrantLock();
    // The maximum time (in ms) the write thread waits before flushing a partial batch.
    private long groupWaitMs;
    // Once the in-flight batch reaches this many bytes the write thread issues an fsync.
    private long flushBytesThreshold;
    // Once the in-flight batch reaches this many entries the write thread issues an fsync.
    private int flushEntriesThreshold;
    // Max write throughput in bytes/s for the write thread. Long.MAX_VALUE means unlimited.
    private long writeBandwidthLimit = Long.MAX_VALUE;

    /**
     * Max record bytes per segment file before rolling to a new WAL segment (excluding file headers).
     */
    private long segmentRollThresholdBytes = 1L << 30;
    private File directory;
    private int timeoutSeconds;
    private boolean recoveryMode;
    private boolean firstStart;
    private int nodeId = NOOP_NODE_ID;
    private long epoch = NOOP_EPOCH;
    private SegmentManager segmentManager;
    private WalMetadataFile walMetadataFile;
    private FilesystemWALHeader walHeader;
    /**
     * Exclusive end offset of the next append (logical WAL byte offset).
     */
    private long nextAppendOffset = 0;

    private FilesystemWALService() {
        appendWorkQueue = new BlockingMpscQueue<>(DEFAULT_APPEND_QUEUE_CAPACITY);
        pendingFsyncQueue = new BlockingMpscQueue<>(DEFAULT_APPEND_QUEUE_CAPACITY);
    }

    public static FilesystemWALServiceBuilder builder(String path) {
        return new FilesystemWALServiceBuilder(path);
    }

    public static FilesystemWALServiceBuilder builder(IdURI uri) {
        FilesystemWALServiceBuilder builder = new FilesystemWALServiceBuilder(uri.path());
        Optional.ofNullable(uri.extensionString("groupWaitMs")).filter(StringUtils::isNumeric).ifPresent(v -> builder.groupWaitMs(Long.parseLong(v)));
        Optional.ofNullable(uri.extensionString("flushBytes")).filter(StringUtils::isNumeric).ifPresent(v -> builder.flushBytesThreshold(Long.parseLong(v)));
        Optional.ofNullable(uri.extensionString("flushEntries")).filter(StringUtils::isNumeric).ifPresent(v -> builder.flushEntriesThreshold(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("writeBuffer")).filter(StringUtils::isNumeric).ifPresent(v -> builder.writeBufferCapacity(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("iobandwidth")).filter(StringUtils::isNumeric).ifPresent(v -> builder.writeBandwidthLimit(Long.parseLong(v)));
        Optional.ofNullable(uri.extensionString("segmentRollThresholdBytes")).filter(StringUtils::isNumeric).ifPresent(v -> builder.segmentRollThresholdBytes(Long.parseLong(v)));
        Optional.ofNullable(uri.extensionString("timeout")).filter(StringUtils::isNumeric).ifPresent(v -> builder.timeoutSeconds(Integer.parseInt(v)));
        return builder;
    }

    public static FilesystemWALServiceBuilder recoveryBuilder(String path) {
        return new FilesystemWALServiceBuilder(path).recoveryMode(true);
    }

    private static boolean shutdownExecutorGracefully(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                return false;
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            return false;
        }
        return true;
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private void flushWALHeader(ShutdownType shutdownType) throws IOException {
        walHeader.setShutdownType(shutdownType);
        flushWALHeader();
    }

    /**
     * Flush the in-memory header into {@link WalMetadataFile} so trim offset and other metadata are
     * durable independently of data segment lifecycle.
     */
    private synchronized void flushWALHeader() throws IOException {
        walMetadataFile.writeHeader(walHeader);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("WAL header flushed to {}: {}", walMetadataFile.file(), walHeader);
        }
    }

    /**
     * Try to read a record starting at the given global offset.
     */
    private ByteBuf readRecord(Segment segment, long recoverStartOffset) throws IOException, ReadRecordException {
        final ByteBuf recordHeader = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        RecordHeader readRecordHeader;
        try {
            readRecordHeader = parseRecordHeader(segment, recoverStartOffset, recordHeader);
        } finally {
            recordHeader.release();
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        ByteBuf recordBody = ByteBufAlloc.byteBuffer(recordBodyLength);
        try {
            parseRecordBody(segment, recoverStartOffset, readRecordHeader, recordBody);
        } catch (Exception e) {
            recordBody.release();
            throw e;
        }

        return recordBody;
    }

    private RecordHeader parseRecordHeader(Segment segment, long recoverStartOffset, ByteBuf recordHeader) throws IOException,
        ReadRecordException {
        int read = segment.readAt(recordHeader, recoverStartOffset, RECORD_HEADER_SIZE);
        if (read != RECORD_HEADER_SIZE) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset),
                String.format("failed to read " + "record header: expected %d bytes, actual %d bytes, " +
                    "recoverStartOffset: %d", RECORD_HEADER_SIZE, read, recoverStartOffset));
        }

        RecordHeader readRecordHeader = RecordHeader.unmarshal(recordHeader);
        if (readRecordHeader.getMagicCode() != RECORD_HEADER_MAGIC_CODE) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset), String.format("magic code " +
                "mismatch: expected %d, actual %d, recoverStartOffset: %d", RECORD_HEADER_MAGIC_CODE,
                readRecordHeader.getMagicCode(), recoverStartOffset));
        }

        int recordHeaderCRC = readRecordHeader.getRecordHeaderCRC();
        int calculatedRecordHeaderCRC = WALUtil.crc32(recordHeader, RECORD_HEADER_WITHOUT_CRC_SIZE);
        if (recordHeaderCRC != calculatedRecordHeaderCRC) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset),
                String.format("record header " + "crc mismatch: expected %d, actual %d, recoverStartOffset: %d",
                    calculatedRecordHeaderCRC, recordHeaderCRC, recoverStartOffset));
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        if (recordBodyLength <= 0) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset),
                String.format("invalid record " + "body length: %d, recoverStartOffset: %d", recordBodyLength,
                    recoverStartOffset));
        }

        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        if (recordBodyOffset != recoverStartOffset + RECORD_HEADER_SIZE) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset),
                String.format("invalid record " + "body offset: expected %d, actual %d, recoverStartOffset: %d",
                    recoverStartOffset + RECORD_HEADER_SIZE, recordBodyOffset, recoverStartOffset));
        }
        return readRecordHeader;
    }

    private void parseRecordBody(Segment segment, long recoverStartOffset, RecordHeader readRecordHeader, ByteBuf recordBody) throws IOException, ReadRecordException {
        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        int read = segment.readAt(recordBody, recordBodyOffset, recordBodyLength);
        if (read != recordBodyLength) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength), String.format("failed to read record body: expected %d bytes, actual %d bytes, recoverStartOffset: %d", recordBodyLength, read, recoverStartOffset));
        }

        int recordBodyCRC = readRecordHeader.getRecordBodyCRC();
        int calculatedRecordBodyCRC = WALUtil.crc32(recordBody);
        if (recordBodyCRC != calculatedRecordBodyCRC) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength), String.format("record body crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordBodyCRC, recordBodyCRC, recoverStartOffset));
        }
    }

    @Override
    public WriteAheadLog start() throws IOException {
        if (started.get()) {
            LOGGER.warn("file system WAL service already started");
            return this;
        }
        StopWatch stopWatch = StopWatch.createStarted();

        walMetadataFile.openOrCreate();
        segmentManager.load();

        FilesystemWALHeader header = tryReadLatestHeader();
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
        this.forceWriteExecutor.submit(new ForceWriteLoop());
        this.writeExecutor.submit(new WriteLoop());
        LOGGER.info("file system WAL service started, cost: {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
        return this;
    }

    private FilesystemWALHeader newWALHeader(long fileStartOffset) {
        return new FilesystemWALHeader(fileStartOffset);
    }

    private FilesystemWALHeader tryReadLatestHeader() throws IOException {
        return walMetadataFile.readLatestHeader();
    }

    private void walHeaderReady(FilesystemWALHeader header) throws IOException {
        if (nodeId != NOOP_NODE_ID) {
            header.setNodeId(nodeId);
            header.setEpoch(epoch);
        }
        this.walHeader = header;
        flushWALHeader();
    }

    private void registerMetrics() {
        S3StreamMetricsManager.registerDeltaWalOffsetSupplier(() -> {
            try {
                return this.nextAppendOffset;
            } catch (Exception e) {
                LOGGER.error("failed to get current start offset", e);
                return 0L;
            }
        }, () -> walHeader.getFlushedTrimOffset());
    }

    @Override
    public void shutdownGracefully() {
        StopWatch stopWatch = StopWatch.createStarted();

        if (!started.getAndSet(false)) {
            LOGGER.warn("file system WAL service already shutdown or not started yet");
            return;
        }

        // Step 1: drain the write pipeline before trim or metadata flush can race with segment close.
        boolean gracefulShutdown =
            shutdownExecutorGracefully(writeExecutor) && shutdownExecutorGracefully(forceWriteExecutor);

        // Step 2: stop the header-flusher; in-flight trim tasks may still delete segments.
        shutdownExecutor(walHeaderFlushExecutor);

        // Step 3: persist final metadata and close data segments.
        try {
            flushWALHeader(gracefulShutdown ? ShutdownType.GRACEFULLY : ShutdownType.UNGRACEFULLY);
        } catch (IOException ignored) {
            // shutdown anyway
        }

        if (segmentManager != null) {
            segmentManager.close();
        }

        if (walMetadataFile != null) {
            walMetadataFile.close();
        }

        LOGGER.info("file system WAL service shutdown gracefully: {}, cost: {} ms", gracefulShutdown,
            stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public WALMetadata metadata() {
        checkStarted();
        return new WALMetadata(walHeader.getNodeId(), walHeader.getEpoch());
    }

    @Override
    public AppendResult append(TraceContext context, ByteBuf buf, int crc) throws OverCapacityException {
        TraceContext.Scope scope = TraceUtils.createAndStartSpan(context, "FilesystemWALService::append");
        final long startTime = System.nanoTime();
        try {
            AppendResult result = append0(buf, crc);
            result.future().whenComplete((nil, ex) -> TraceUtils.endSpan(scope, ex));
            return result;
        } catch (Throwable t) {
            if (t instanceof OverCapacityException) {
                StorageOperationStats.getInstance().appendWALFullStats.record(TimerUtil.timeElapsedSince(startTime,
                    TimeUnit.NANOSECONDS));
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
        QueueEntry entry = new QueueEntry();
        CompletableFuture<AppendResult.CallbackResult> timedFuture =
            FutureUtil.timeoutWithNewReturn(appendResultFuture, timeoutSeconds, TimeUnit.SECONDS,
                () -> failed.set(true));
        long walOffset = 0;
        appendOffsetLock.lock();
        try {
            walOffset = nextAppendOffset;
            ByteBuf header = HEADER_POOL.get().retain();
            entry.setWalOffset(walOffset);
            entry.setRecord(WALUtil.generateRecord(body, header, crc, walOffset));
            entry.setFuture(appendResultFuture);
            appendWorkQueue.put(entry);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Failed to put entry to append work queue", e);
            AppendResultImpl errRes = new AppendResultImpl(walOffset, timedFuture);
            errRes.future().completeExceptionally(e);
            return errRes;
        } finally {
            this.nextAppendOffset += recordSize;
            appendOffsetLock.unlock();
        }
        final AppendResult appendResult = new AppendResultImpl(walOffset, timedFuture);
        appendResult.future().whenComplete((nil, ex) -> StorageOperationStats.getInstance().appendWALCompleteStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS)));
        StorageOperationStats.getInstance().appendWALBeforeStats.record(TimerUtil.timeElapsedSince(startTime,
            TimeUnit.NANOSECONDS));
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
        return new RecoverIterator(recoverStartOffset);
    }

    @Override
    public CompletableFuture<Void> reset() {
        checkStarted();
        // Pick a fresh start offset beyond all existing data so we begin writing into a clean
        // segment after the existing data.
        long current = walHeader.getTrimOffset();
        long highestEnd = segmentManager.highestKnownEndOffset();
        long newStartOffset = Math.max(current + 1, highestEnd);
        CompletableFuture<Void> cf = trim(newStartOffset - 1, true).thenRun(() -> {
            appendOffsetLock.lock();
            try {
                nextAppendOffset = newStartOffset;
                segmentManager.clearCurrentSegment();
                resetFinished.set(true);
            } finally {
                appendOffsetLock.unlock();
            }
        });

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
        }

        walHeader.updateTrimOffset(offset);
        return CompletableFuture.runAsync(() -> {
            try {
                flushWALHeader();
                // After the trim offset is durable, segments fully covered by it can be removed.
                int deleted = segmentManager.deleteSegmentsBelowOrEqual(Math.min(flushedMarkOffset.get(),
                    walHeader.getFlushedTrimOffset()));
                if (deleted > 0) {
                    LOGGER.info("removed {} fully-trimmed segment file(s)", deleted);
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }, walHeaderFlushExecutor);
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

    public static class FilesystemWALServiceBuilder {
        private final String directoryPath;
        private long segmentRollThresholdBytes = 1L << 30;
        /**
         * Capacity of each {@link Segment}'s {@link BufferedChannel} write buffer in bytes.
         */
        private int writeBufferCapacity = Segment.DEFAULT_WRITE_BUFFER_CAPACITY;
        // The maximum time (in ms) the write thread waits before flushing a partial batch.
        private long groupWaitMs = 2;
        // Once the in-flight batch reaches this many bytes the write thread issues an fsync.
        private long flushBytesThreshold = 4L << 20; // 4 MiB
        // Once the in-flight batch reaches this many entries the write thread issues an fsync.
        private int flushEntriesThreshold = 1024;
        // Max write throughput in bytes/s. Long.MAX_VALUE means unlimited.
        private long writeBandwidthLimit = Long.MAX_VALUE;
        private int timeoutSeconds = 30;
        private int nodeId = NOOP_NODE_ID;
        private long epoch = NOOP_EPOCH;
        private boolean recoveryMode = false;

        public FilesystemWALServiceBuilder(String directoryPath) {
            this.directoryPath = directoryPath;
        }

        public FilesystemWALServiceBuilder recoveryMode(boolean recoveryMode) {
            this.recoveryMode = recoveryMode;
            return this;
        }

        public FilesystemWALServiceBuilder config(Config config) {
            return this.nodeId(config.nodeId()).epoch(config.nodeEpoch());
        }

        /**
         * When the cumulative record bytes in the active segment reach this threshold, the next append
         * starts a new segment file. Larger values mean fewer, bigger files; smaller values increase
         * file count and improve trim granularity.
         */
        public FilesystemWALServiceBuilder segmentRollThresholdBytes(long segmentRollThresholdBytes) {
            this.segmentRollThresholdBytes = segmentRollThresholdBytes;
            return this;
        }

        /**
         * Size of each segment file's in-memory write buffer ({@link BufferedChannel} capacity).
         */
        public FilesystemWALServiceBuilder writeBufferCapacity(int writeBufferBytes) {
            if (writeBufferBytes <= 0) {
                throw new IllegalArgumentException("writeBufferBytes must be positive: " + writeBufferBytes);
            }
            this.writeBufferCapacity = writeBufferBytes;
            return this;
        }

        public FilesystemWALServiceBuilder groupWaitMs(long groupWaitMs) {
            this.groupWaitMs = groupWaitMs;
            return this;
        }

        public FilesystemWALServiceBuilder flushBytesThreshold(long flushBytesThreshold) {
            this.flushBytesThreshold = flushBytesThreshold;
            return this;
        }

        public FilesystemWALServiceBuilder flushEntriesThreshold(int flushEntriesThreshold) {
            this.flushEntriesThreshold = flushEntriesThreshold;
            return this;
        }

        public FilesystemWALServiceBuilder writeBandwidthLimit(long writeBandwidthLimit) {
            if (writeBandwidthLimit <= 0) {
                throw new IllegalArgumentException("writeBandwidthLimit must be positive: " + writeBandwidthLimit);
            }
            this.writeBandwidthLimit = writeBandwidthLimit;
            return this;
        }

        public FilesystemWALServiceBuilder timeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            return this;
        }

        public FilesystemWALServiceBuilder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public FilesystemWALServiceBuilder epoch(long epoch) {
            this.epoch = epoch;
            return this;
        }

        public FilesystemWALService build() {
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

            FilesystemWALService service = new FilesystemWALService();
            service.directory = new File(directoryPath);
            service.walMetadataFile = new WalMetadataFile(service.directory);
            service.segmentManager = new SegmentManager(service.directory, writeBufferCapacity);
            service.segmentRollThresholdBytes = segmentRollThresholdBytes;
            service.timeoutSeconds = timeoutSeconds;
            service.recoveryMode = recoveryMode;

            service.groupWaitMs = groupWaitMs;
            service.flushBytesThreshold = flushBytesThreshold;
            service.flushEntriesThreshold = flushEntriesThreshold;
            service.writeBandwidthLimit = writeBandwidthLimit;
            if (nodeId != NOOP_NODE_ID) {
                service.nodeId = nodeId;
                service.epoch = epoch;
            }

            LOGGER.info("build FilesystemWALService: {}", this);
            return service;
        }
    }

    private static final class ReadRecordException extends Exception {
        private final long jumpNextRecoverOffset;

        private ReadRecordException(long offset, String message) {
            super(message);
            this.jumpNextRecoverOffset = offset;
        }

        private long getJumpNextRecoverOffset() {
            return jumpNextRecoverOffset;
        }
    }

    private static final class QueueEntry {
        private long walOffset;
        private Record record;
        private CompletableFuture<AppendResult.CallbackResult> future;

        public long getWalOffset() {
            return walOffset;
        }

        public void setWalOffset(long walOffset) {
            this.walOffset = walOffset;
        }

        public CompletableFuture<AppendResult.CallbackResult> getFuture() {
            return future;
        }

        public void setFuture(CompletableFuture<AppendResult.CallbackResult> future) {
            this.future = future;
        }

        public Record getRecord() {
            return record;
        }

        public void setRecord(Record record) {
            this.record = record;
        }

        @FunctionalInterface
        private interface RecordSupplier {
            /**
             * Generate a record.
             *
             * @param recordStartOffset The start offset of this record.
             * @param emptyHeader       An empty {@link ByteBuf} with the size of
             *                          {@link RecordHeader#RECORD_HEADER_SIZE}
             *                          . It will be used to marshal the header.
             * @return The record.
             */
            Record get(long recordStartOffset, ByteBuf emptyHeader);
        }
    }

    /**
     * A batch of writes whose payload has been handed to the OS and is waiting for fsync.
     */
    private static final class ForceWriteRequest {
        private final Segment segment;
        private final long startOffset;
        private final long endOffset;
        private final List<CompletableFuture<AppendResult.CallbackResult>> futures;
        private final boolean shouldClose;

        private ForceWriteRequest(Segment segment, long startOffset, long endOffset,
                                  List<CompletableFuture<AppendResult.CallbackResult>> futures, boolean shouldClose) {
            this.segment = segment;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.futures = futures;
            this.shouldClose = shouldClose;
        }

        public void process() {
            if (shouldClose) {
                segment.close();
            }
        }
    }

    /**
     * Single-threaded write stage: consumes {@link QueueEntry} items, writes each record to the
     * right segment (no fsync) and hands off complete batches to the force-write thread.
     */
    private final class WriteLoop implements Runnable {
        private final List<CompletableFuture<AppendResult.CallbackResult>> batchFutures = new ArrayList<>();
        private final RateLimiter writeBandwidthLimiter = writeBandwidthLimit == Long.MAX_VALUE
            ? null : RateLimiter.create((double) writeBandwidthLimit);
        private Segment batchSegment = null;
        private long batchStartOffset = -1;
        private long batchEndOffset = -1;
        private long batchBytes = 0;
        private long batchStartNanos = 0;
        private int batchEntries = 0;

        @Override
        public void run() {
            LOGGER.info("file-wal write thread started");
            try {
                while (started.get()) {
                    QueueEntry entry;
                    try {
                        // Timed wait so a partial batch can be flushed when producers go idle and
                        // byte/entry thresholds are not reached.
                        entry = appendWorkQueue.poll(groupWaitMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    if (entry != null) {
                        writeQueuedRecord(entry);
                    }
                    maybeFlushBatch();
                }
            } catch (Throwable t) {
                LOGGER.error("file-wal write thread terminated unexpectedly", t);
            } finally {
                // Drain anything we haven't handed off yet.
                drainRemainingOnShutdown();
                LOGGER.info("file-wal write thread exited");
            }
        }


        /**
         * Takes one {@link QueueEntry} from the append pipeline, writes its record bytes to the
         * correct segment (no fsync yet), and folds it into the batch handed to the force-write thread.
         */
        private void writeQueuedRecord(QueueEntry entry) {
            final long walOffset = entry.getWalOffset();
            final Record record = entry.getRecord();
            CompositeByteBuf data = null;
            try {
                Segment segment = segmentManager.latestSegment(walOffset);
                data = ByteBufAlloc.compositeByteBuffer();
                data.addComponents(true, record.header(), record.body());
                final int payloadBytes = data.readableBytes();
                if (writeBandwidthLimiter != null) {
                    writeBandwidthLimiter.acquire(payloadBytes);
                }
                final long writeStart = System.nanoTime();
                segment.write(data);
                StorageOperationStats.getInstance().appendWALWriteStats.record(TimerUtil.timeElapsedSince(writeStart,
                    TimeUnit.NANOSECONDS));

                trackRecordInOpenBatch(entry, segment, payloadBytes);

                //segmentForAppend(walOffset);
                boolean shouldClose = segment.getPosition() > segmentRollThresholdBytes;
                if (shouldClose) {
                    flushBatchToForceWrite(true);
                    segmentManager.rollOverSegment(walOffset + payloadBytes);
                }
            } catch (Exception e) {
                LOGGER.error("failed to write block, walOffset: {}", walOffset, e);
                entry.getFuture().completeExceptionally(e);
            } finally {
                // Payload has been copied into the file channel; release views. Append futures are
                // completed after fsync on the force-write thread.
                if (data != null) {
                    data.release();
                }
                HEADER_POOL.release(record.header());
            }
        }

        private void trackRecordInOpenBatch(QueueEntry entry, Segment segment, int payloadBytes) {
            batchSegment = segment;
            if (batchEntries == 0) {
                batchStartNanos = System.nanoTime();
                batchStartOffset    = entry.walOffset;
            }
            batchEndOffset = entry.getWalOffset() + payloadBytes;
            batchBytes += payloadBytes;
            batchEntries++;
            batchFutures.add(entry.getFuture());
        }

        private void maybeFlushBatch() {
            if (batchEntries == 0) {
                return;
            }

            boolean timeout = System.nanoTime() - batchStartNanos >= TimeUnit.MILLISECONDS.toNanos(groupWaitMs);
            boolean overBytes = batchBytes >= flushBytesThreshold;
            boolean overEntries = batchEntries >= flushEntriesThreshold;
            if (timeout || overBytes || overEntries) {
                flushBatchToForceWrite(false);
            }
        }

        private void flushBatchToForceWrite(boolean shouldClose) {
            if (batchEntries == 0) {
                return;
            }
            ForceWriteRequest req = new ForceWriteRequest(batchSegment, batchStartOffset, batchEndOffset,
                new ArrayList<>(batchFutures), shouldClose);
            try {
                pendingFsyncQueue.put(req);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                FutureUtil.completeExceptionally(req.futures.iterator(), new WALShutdownException("interrupted while "
                    + "enqueuing force-write request"));
            }
            batchFutures.clear();
            batchSegment = null;
            batchStartOffset = -1;
            batchEndOffset = -1;
            batchBytes = 0;
            batchEntries = 0;
            batchStartNanos = 0;
        }

        private void drainRemainingOnShutdown() {
            // Pick up any queue entries still waiting so callers don't hang.
            while (true) {
                QueueEntry entry = appendWorkQueue.poll();
                if (entry == null) {
                    break;
                }
                writeQueuedRecord(entry);
            }
            flushBatchToForceWrite(true);
        }
    }

    /**
     * Single-threaded force-write stage: groups requests, fsyncs each distinct segment exactly
     * once per batch, completes pending futures and advises the kernel to drop the corresponding
     * page cache range.
     * <p>
     * The loop continues while {@code started} is true <em>or</em> while {@link #writeExecutor} is still
     * running.  This is necessary because {@link WriteLoop#drainRemainingOnShutdown()} pushes a
     * final batch to {@code pendingFsyncQueue} after {@code started} is set to {@code false}; if
     * this loop exited as soon as {@code started} became false it would miss that final batch and
     * leave the associated futures unresolved.  Once {@link #writeExecutor} has terminated no new items
     * can be added, so a final non-blocking drain in the {@code finally} block catches anything
     * that arrived in the window between the last timed poll and the loop exit.
     */
    private final class ForceWriteLoop implements Runnable {

        @Override
        public void run() {
            LOGGER.info("file-wal force-write thread started");
            @SuppressWarnings("unchecked") ForceWriteRequest[] batch =
                new ForceWriteRequest[FSYNC_COALESCE_BUFFER_CAPACITY];
            try {
                while (started.get() || !writeExecutor.isTerminated()) {
                    try {
                        int count = pendingFsyncQueue.pollAll(batch, groupWaitMs, TimeUnit.MILLISECONDS);
                        if (count == 0) {
                            continue;
                        }
                        processForceWriteBatch(batch, count);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        started.set(false);
                        break;
                    } catch (Throwable t) {
                        started.set(false);
                        LOGGER.error("unexpected error in force-write thread", t);
                        break;
                    }
                }
            } finally {
                // Drain any items deposited by WriteLoop.drainRemainingOnShutdown() after the main
                // loop exited. writeExecutor has terminated (or we broke out on error) so no further
                // items can be added.
                drainRemainingOnShutdown(batch);
                LOGGER.info("file-wal force-write thread exited");
            }
        }

        private void drainRemainingOnShutdown(ForceWriteRequest[] batch) {
            while (true) {
                int count = 0;
                ForceWriteRequest req;
                while (count < batch.length && (req = pendingFsyncQueue.poll()) != null) {
                    batch[count++] = req;
                }
                if (count == 0) {
                    break;
                }
                processForceWriteBatch(batch, count);
            }
        }

        private void processForceWriteBatch(ForceWriteRequest[] batch, int count) {
            // Group by segment so each segment is fsynced exactly once per batch.
            Arrays.stream(batch, 0, count).collect(Collectors.groupingBy(r -> r.segment)).forEach((segment, requests) -> {
                ForceWriteRequest first = requests.get(0);
                ForceWriteRequest last = requests.get(requests.size() - 1);
                try {
                    segment.fsync();
                    // Capture the durable end offset at the moment of the fsync so that
                    // flushedOffset() always returns a stable, already-synced value.
                    flushedMarkOffset.set(last.endOffset);
                    requests.forEach(r -> {
                        r.process();
                        r.futures.forEach(f -> f.complete(() -> first.startOffset));
                    });
                } catch (Throwable t) {
                    requests.forEach(r -> r.futures.forEach(f -> f.completeExceptionally(t)));
                    LOGGER.error("unexpected error in force-write thread", t);
                }
            });
        }
    }

    /**
     * Linear scan recovery: walks forward from the first un-trimmed offset, decoding records and
     * jumping over corrupted ones.
     */
    protected class RecoverIterator implements Iterator<RecoverResult> {
        private long nextRecoverOffset;
        private long lastValidOffset = -1;
        private boolean firstInvalidSeen;
        private long firstInvalidOffset = -1;
        private RecoverResult next;

        public RecoverIterator(long nextRecoverOffset) {
            this.nextRecoverOffset = nextRecoverOffset;
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
                try {
                    ByteBuf nextRecordBody = readRecord(segment, nextRecoverOffset);
                    RecoverResultImpl recoverResult = new RecoverResultImpl(nextRecordBody, nextRecoverOffset);
                    lastValidOffset = nextRecoverOffset;
                    nextRecoverOffset += RECORD_HEADER_SIZE + nextRecordBody.readableBytes();
                    next = recoverResult;
                    return true;
                } catch (ReadRecordException e) {
                    if (!firstInvalidSeen && WALUtil.isAligned(nextRecoverOffset)) {
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
                return segmentManager.segmentForOffset(nextRecoverOffset) != null || segmentManager.nextSegmentAfter(nextRecoverOffset) != null;
            }
            // After the first invalid record we still look a little further to tolerate scattered
            // bad records, but stop once we have walked past the window.
            long slack = 1 << 22;
            return nextRecoverOffset < (lastValidOffset >= 0 ? lastValidOffset : firstInvalidOffset) + slack;
        }
    }

}
