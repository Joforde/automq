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
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
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
    /** Capacity of the append work queue and of the queue feeding the fsync coalescer. */
    private static final int DEFAULT_APPEND_QUEUE_CAPACITY = 1000;
    /** Max number of {@link ForceWriteRequest} items pulled per timed poll in {@link ForceWriteLoop}. */
    private static final int FSYNC_COALESCE_BUFFER_CAPACITY = 1000;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean resetFinished = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final ExecutorService walHeaderFlushExecutor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
        "flush-file-wal-header-thread-%d", true), LOGGER);

    private final ExecutorService writeExecutor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory("file" +
        "-wal-write-thread-%d", true), LOGGER);

    private final ExecutorService forceWriteExecutor = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
        "file-wal-force-write-thread-%d", true), LOGGER);

    private final BatchedBlockingQueue<QueueEntry> appendWorkQueue;
    private final BatchedBlockingQueue<ForceWriteRequest> pendingFsyncQueue;
    /** Serializes assignment of {@link #nextAppendOffset} (next record's logical start offset). */
    private final ReentrantLock appendOffsetLock = new ReentrantLock();
    // The maximum time (in ms) the write thread waits before flushing a partial batch.
    private long groupWaitMs;
    // Once the in-flight batch reaches this many bytes the write thread issues an fsync.
    private long flushBytesThreshold;
    // Once the in-flight batch reaches this many entries the write thread issues an fsync.
    private int flushEntriesThreshold;

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
    /** Exclusive end offset of the next append (logical WAL byte offset). */
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
        Optional.ofNullable(uri.extensionString("timeout")).filter(StringUtils::isNumeric).ifPresent(v -> builder.timeoutSeconds(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("segmentRollThresholdBytes")).filter(StringUtils::isNumeric).ifPresent(v -> builder.segmentRollThresholdBytes(Long.parseLong(v)));
        return builder;
    }

    public static FilesystemWALServiceBuilder recoveryBuilder(String path) {
        return new FilesystemWALServiceBuilder(path).recoveryMode(true);
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

    private RecordHeader parseRecordHeader(long recoverStartOffset, ByteBuf recordHeader) throws IOException,
        ReadRecordException {
        Segment segment = segmentManager.segmentForOffset(recoverStartOffset);
        if (segment == null) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset),
                String.format("no segment for " + "offset %d", recoverStartOffset));
        }
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

    private void parseRecordBody(long recoverStartOffset, RecordHeader readRecordHeader, ByteBuf recordBody) throws IOException, ReadRecordException {
        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        Segment segment = segmentManager.segmentForOffset(recordBodyOffset);
        if (segment == null) {
            throw new ReadRecordException(WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength), String.format("no segment for record body offset %d", recordBodyOffset));
        }
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
        boolean gracefulShutdown = shutdownExecutorGracefully(writeExecutor)
            && shutdownExecutorGracefully(forceWriteExecutor);

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
        Supplier<Record> supplier;
        final long recordStartOffset;
        appendOffsetLock.lock();
        try {
            QueueEntry.RecordSupplier recordSupplier = (offset, header) -> WALUtil.generateRecord(body, header, crc,
                offset);
            recordStartOffset = nextAppendOffset;
            nextAppendOffset += recordSize;
            supplier = () -> {
                ByteBuf header = HEADER_POOL.get().retain();
                return recordSupplier.get(recordStartOffset, header);
            };
            entry.setStartOffset(recordStartOffset);
        } finally {
            appendOffsetLock.unlock();
        }
        entry.setRecord(supplier.get());
        entry.setFuture(appendResultFuture);

        CompletableFuture<AppendResult.CallbackResult> timedFuture =
            FutureUtil.timeoutWithNewReturn(appendResultFuture, timeoutSeconds, TimeUnit.SECONDS,
                () -> failed.set(true));
        final AppendResult appendResult = new AppendResultImpl(recordStartOffset, timedFuture);

        try {
            appendWorkQueue.put(entry);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Failed to put entry to append work queue", e);
            appendResult.future().completeExceptionally(e);
            return appendResult;
        }
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
        return new RecoverIterator(recoverStartOffset, trimmedOffset);
    }

    @Override
    public CompletableFuture<Void> reset() {
        checkStarted();

        // Pick a fresh start offset beyond all existing data so we begin writing into a clean
        // segment after the existing data.
        long current = walHeader.getTrimOffset();
        long highestEnd = segmentManager.highestKnownEndOffset();
        long newStartOffset = Math.max(current + 1, highestEnd);

        LOGGER.info("reset sliding window to offset: {}", newStartOffset);
        CompletableFuture<Void> cf = trim(newStartOffset - 1, true).thenRun(() -> resetFinished.set(true));

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
                awaitPendingFsyncDrained();
                flushWALHeader();
                // After the trim offset is durable, segments fully covered by it can be removed.
                int deleted = segmentManager.deleteSegmentsBelowOrEqual(walHeader.getFlushedTrimOffset());
                if (deleted > 0) {
                    LOGGER.info("removed {} fully-trimmed segment file(s)", deleted);
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeIOException(new IOException("trim interrupted", e));
            }
        }, walHeaderFlushExecutor);
    }

    /**
     * Wait until all pending segment fsync work has finished so trimmed segments are not closed
     * while {@link ForceWriteLoop} still holds references to them.
     */
    private void awaitPendingFsyncDrained() throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadlineNanos) {
            if (pendingFsyncQueue.isEmpty()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        throw new RuntimeIOException(new IOException("timed out waiting for pending WAL fsync to drain"));
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
        // The maximum time (in ms) the write thread waits before flushing a partial batch.
        private long groupWaitMs = 2;
        // Once the in-flight batch reaches this many bytes the write thread issues an fsync.
        private long flushBytesThreshold = 4L << 20; // 4 MiB
        // Once the in-flight batch reaches this many entries the write thread issues an fsync.
        private int flushEntriesThreshold = 1024;
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
            service.segmentManager = new SegmentManager(service.directory);
            service.segmentRollThresholdBytes = segmentRollThresholdBytes;
            service.timeoutSeconds = timeoutSeconds;
            service.recoveryMode = recoveryMode;

            service.groupWaitMs = groupWaitMs;
            service.flushBytesThreshold = flushBytesThreshold;
            service.flushEntriesThreshold = flushEntriesThreshold;
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
        private long startOffset;
        private Record record;
        private CompletableFuture<AppendResult.CallbackResult> future;

        public long getStartOffset() {
            return startOffset;
        }

        public void setStartOffset(long startOffset) {
            this.startOffset = startOffset;
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
             * {@link RecordHeader#RECORD_HEADER_SIZE}
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
        private final long firstOffset;
        private final long endOffset;
        private final List<CompletableFuture<AppendResult.CallbackResult>> futures;

        private ForceWriteRequest(Segment segment, long firstOffset, long endOffset,
                          List<CompletableFuture<AppendResult.CallbackResult>> futures) {
            this.segment = segment;
            this.firstOffset = firstOffset;
            this.endOffset = endOffset;
            this.futures = futures;
        }
    }

    /**
     * Single-threaded write stage: consumes {@link QueueEntry} items, writes each record to the
     * right segment (no fsync) and hands off complete batches to the force-write thread.
     */
    private final class WriteLoop implements Runnable {
        private final List<CompletableFuture<AppendResult.CallbackResult>> batchFutures = new ArrayList<>();
        private Segment batchSegment = null;
        private long batchFirstOffset = -1;
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
            final long startOffset = entry.getStartOffset();
            final Record record = entry.getRecord();
            CompositeByteBuf data = null;
            try {
                Segment segment = segmentForAppend(startOffset);
                finishBatchBeforeSwitchingTo(segment);

                data = ByteBufAlloc.compositeByteBuffer();
                data.addComponents(true, record.header(), record.body());
                final int payloadBytes = data.readableBytes();

                final long writeStart = System.nanoTime();
                segment.writeAt(data, startOffset);
                StorageOperationStats.getInstance().appendWALWriteStats.record(
                    TimerUtil.timeElapsedSince(writeStart, TimeUnit.NANOSECONDS));

                trackRecordInOpenBatch(entry, segment, startOffset, payloadBytes);
            } catch (Exception e) {
                LOGGER.error("failed to write block, startOffset: {}", startOffset, e);
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

        /**
         * Returns the segment that should receive {@code startOffset}. When the append lands exactly
         * at the end of the current on-disk file and that file already holds
         * {@code segmentRollThresholdBytes} of record bytes, the current batch is handed off and a
         * new segment file is created at the same global offset.
         */
        private Segment segmentForAppend(long startOffset) throws IOException {
            Segment segment = segmentManager.getOrCreateSegmentForOffset(startOffset);
            if (!needsSegmentRoll(segment, startOffset)) {
                return segment;
            }
            flushBatchToForceWrite();
            return segmentManager.createSegment(startOffset);
        }

        /**
         * {@code true} when this append is the next byte after the written range and the written
         * range has reached the configured roll threshold (exclusive end == {@code startOffset}).
         */
        private boolean needsSegmentRoll(Segment segment, long startOffset) {
            long writtenEndExclusive = segment.endOffsetExclusive();
            if (startOffset != writtenEndExclusive) {
                return false;
            }
            long recordBytesInFile = writtenEndExclusive - segment.startOffset();
            return recordBytesInFile >= segmentRollThresholdBytes;
        }

        /**
         * A single {@link ForceWriteRequest} targets one segment; flush before mixing files.
         */
        private void finishBatchBeforeSwitchingTo(Segment segment) {
            if (batchSegment != null && batchSegment != segment) {
                flushBatchToForceWrite();
            }
        }

        private void trackRecordInOpenBatch(QueueEntry entry, Segment segment, long startOffset, int payloadBytes) {
            batchSegment = segment;
            if (batchEntries == 0) {
                batchFirstOffset = startOffset;
                batchStartNanos = System.nanoTime();
            }
            batchEndOffset = startOffset + payloadBytes;
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
                flushBatchToForceWrite();
            }
        }

        private void flushBatchToForceWrite() {
            if (batchEntries == 0) {
                return;
            }
            ForceWriteRequest req = new ForceWriteRequest(batchSegment, batchFirstOffset, batchEndOffset,
                new ArrayList<>(batchFutures));
            try {
                pendingFsyncQueue.put(req);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                FutureUtil.completeExceptionally(req.futures.iterator(), new WALShutdownException("interrupted while "
                    + "enqueuing force-write request"));
            }
            batchFutures.clear();
            batchSegment = null;
            batchFirstOffset = -1;
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
            flushBatchToForceWrite();
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
            @SuppressWarnings("unchecked") ForceWriteRequest[] batch = new ForceWriteRequest[FSYNC_COALESCE_BUFFER_CAPACITY];
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
                    if (!segment.isOpen()) {
                        throw new IOException("segment already closed: " + segment);
                    }
                    segment.fsync();
                    // Capture the durable end offset at the moment of the fsync so that
                    // flushedOffset() always returns a stable, already-synced value.
                    final long flushedOffset = last.endOffset;
                    requests.forEach(r -> r.futures.forEach(f -> f.complete(() -> flushedOffset)));
                } catch (Throwable t) {
                    requests.forEach(r -> r.futures.forEach(f -> f.completeExceptionally(t)));
                    LOGGER.error("unexpected error in force-write thread", t);
                }
                segment.adviseDontNeedRange(first.firstOffset, last.endOffset - first.firstOffset);
            });
        }
    }

    /**
     * Linear scan recovery: walks forward from the first un-trimmed offset, decoding records and
     * jumping over corrupted ones.
     */
    protected class RecoverIterator implements Iterator<RecoverResult> {
        private final long skipRecordAtOffset;
        private long nextRecoverOffset;
        private long lastValidOffset = -1;
        private boolean firstInvalidSeen;
        private long firstInvalidOffset = -1;
        private RecoverResult next;

        public RecoverIterator(long nextRecoverOffset, long skipRecordAtOffset) {
            this.nextRecoverOffset = nextRecoverOffset;
            this.skipRecordAtOffset = skipRecordAtOffset;
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
                return segmentManager.segmentForOffset(nextRecoverOffset) != null || segmentManager.nextSegmentAfter(nextRecoverOffset) != null;
            }
            // After the first invalid record we still look a little further to tolerate scattered
            // bad records, but stop once we have walked past the window.
            long slack = 1 << 22;
            return nextRecoverOffset < (lastValidOffset >= 0 ? lastValidOffset : firstInvalidOffset) + slack;
        }
    }

}
