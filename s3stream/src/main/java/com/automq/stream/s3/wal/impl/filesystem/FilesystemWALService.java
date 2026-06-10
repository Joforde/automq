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
import com.automq.stream.s3.wal.common.BatchedBlockingQueue;
import com.automq.stream.s3.wal.common.BlockingMpscQueue;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.common.RecoverResultImpl;
import com.automq.stream.s3.wal.common.ShutdownType;
import com.automq.stream.s3.wal.common.WALMetadata;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.RuntimeIOException;
import com.automq.stream.s3.wal.exception.WALShutdownException;
import com.automq.stream.s3.wal.impl.block.Block;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.IdURI;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.github.bucket4j.BlockingBucket;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import io.github.bucket4j.Bucket;

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
 * Append pipeline (similar in spirit to {@code BlockWALService}):
 * <ol>
 *   <li>{@link #append} takes the {@code blockLock}, hands the payload to
 *       {@link FilesystemSlidingWindowService}'s current {@link Block} and returns immediately -
 *       record header marshalling, segment selection, file I/O and fsync all happen out of the
 *       critical section.</li>
 *   <li>The {@code block-poll} thread drains blocks from the sliding window, materialises their
 *       bytes via {@link Block#data()}, picks the destination segment and enqueues a
 *       {@link WriteRequest} on the write queue.</li>
 *   <li>The {@code write} thread consumes the write queue, writes each block to its segment
 *       (without fsync), coalesces consecutive writes targeting the same segment into a batch
 *       and hands the batch to the force-write thread for fsync.</li>
 *   <li>The {@code force-write} thread fsyncs the segment, completes the corresponding append
 *       futures and releases the underlying buffers.</li>
 * </ol>
 */
public class FilesystemWALService implements WriteAheadLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemWALService.class);
    /**
     * Capacity of the write queue and of the queue feeding the fsync coalescer.
     */
    private static final int DEFAULT_PIPELINE_QUEUE_CAPACITY = 20;
    /**
     * Max number of {@link ForceWriteRequest} items pulled per timed poll in {@link ForceWriteLoop}.
     */
    private static final int FSYNC_COALESCE_BUFFER_CAPACITY = 20;
    /**
     * Log a warning when a pipeline queue reaches this fraction of its capacity.
     */
    private static final int PIPELINE_QUEUE_WARN_THRESHOLD = DEFAULT_PIPELINE_QUEUE_CAPACITY * 3 / 4;
    private static final int PIPELINE_MONITOR_INTERVAL_SECONDS = 5;

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
    private final BatchedBlockingQueue<ForceWriteRequest> forceQueue;
    // Max write throughput in bytes/s for the write thread. Long.MAX_VALUE means unlimited.
    private long writeBandwidthLimit = Long.MAX_VALUE;
    // Max Segment.write calls per second.
    private int writeRateLimit = Integer.MAX_VALUE;
    // Max fsync calls per second in ForceWriteLoop.
    private int fsyncRateLimit = Integer.MAX_VALUE;
    // Max payload bytes per block before it must be sealed and a new block opened.
    private long blockMaxSize;
    // Soft size limit per block - the block can exceed this only when it contains a single oversized record.
    private long blockSoftLimit;

    private ExecutorService callbackExecutor;


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
    private FilesystemSlidingWindowService slidingWindowService;
    private Bucket writeRateBucket;
    private BlockingBucket writeBandwidthBucket;
    private Bucket fsyncRateBucket;

    private FilesystemWALService() {
        forceQueue = new BlockingMpscQueue<>(DEFAULT_PIPELINE_QUEUE_CAPACITY);
    }
    protected FilesystemWALService(FilesystemWALService.FilesystemWALServiceBuilder builder) {
        this();
        FilesystemWALService that = builder.build();
        this.slidingWindowService = that.slidingWindowService;
        this.walHeader = that.walHeader;
        this.recoveryMode = that.recoveryMode;
        this.nodeId = that.nodeId;
        this.epoch = that.epoch;
        this.walMetadataFile = that.walMetadataFile;
        this.writeRateLimit = that.writeRateLimit;
        this.writeBandwidthLimit = that.writeBandwidthLimit;
        this.segmentManager = that.segmentManager;
    }

    public static FilesystemWALServiceBuilder builder(String path) {
        return new FilesystemWALServiceBuilder(path);
    }

    public static FilesystemWALServiceBuilder builder(IdURI uri) {
        FilesystemWALServiceBuilder builder = new FilesystemWALServiceBuilder(uri.path());
        Optional.ofNullable(uri.extensionString("segmentRollThresholdBytes")).filter(StringUtils::isNumeric).ifPresent(v -> builder.segmentRollThresholdBytes(Long.parseLong(v)));
        Optional.ofNullable(uri.extensionString("iops")).filter(StringUtils::isNumeric).ifPresent(v -> builder.writeRateLimit(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("fsync")).filter(StringUtils::isNumeric).ifPresent(v -> builder.fsyncRateLimit(Integer.parseInt(v)));
        Optional.ofNullable(uri.extensionString("iobandwidth")).filter(StringUtils::isNumeric).ifPresent(v -> builder.writeBandwidthLimit(Long.parseLong(v)));
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

    private static Bucket buildBucket(long ratePerSecond) {
        if (ratePerSecond <= 0 || ratePerSecond == Long.MAX_VALUE) {
            return null;
        }
        return Bucket.builder().addLimit(limit -> limit.capacity(Math.max(ratePerSecond / 10, 1)).refillGreedy(ratePerSecond, Duration.ofSeconds(1))).build();
    }

    private static BlockingBucket buildBlockingBucket(long ratePerSecond) {
        if (ratePerSecond <= 0 || ratePerSecond == Long.MAX_VALUE) {
            return null;
        }
        return Bucket.builder().addLimit(limit -> limit.capacity(Math.max(ratePerSecond / 10, 1)).refillGreedy(ratePerSecond, Duration.ofSeconds(1))).build().asBlocking();
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

    private RecordHeader parseRecordHeader(Segment segment, long recoverStartOffset, ByteBuf recordHeader) throws IOException, ReadRecordException {
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

    private void parseRecordBody(Segment segment, long recoverStartOffset, RecordHeader readRecordHeader,
                                 ByteBuf recordBody) throws IOException, ReadRecordException {
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
        this.callbackExecutor = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(8, "wal-callback-thread", false,
            LOGGER);
        this.forceWriteExecutor.submit(new ForceWriteLoop());
        this.writeExecutor.submit(new WriteLoop());
        started.set(true);
        LOGGER.info("file system WAL service started, cost: {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
        return this;
    }

    private FilesystemWALHeader newWALHeader(long fileStartOffset) {
        return new FilesystemWALHeader(fileStartOffset);
    }

    public FilesystemWALHeader tryReadLatestHeader() throws IOException {
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
                Lock lock = slidingWindowService.getBlockLock();
                lock.lock();
                try {
                    return slidingWindowService.nextAppendOffsetLocked();
                } finally {
                    lock.unlock();
                }
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

        // Step 1: drain the append pipeline in stage order. Each stage's drain happens in its
        // shutdown finally block so that everything queued before {@code started=false} is
        // delivered to the next stage.
        boolean gracefulShutdown =
            shutdownExecutorGracefully(callbackExecutor) && shutdownExecutorGracefully(writeExecutor) && shutdownExecutorGracefully(forceWriteExecutor);

        // Step 2: fail any futures still buffered inside the sliding window service that did not
        // make it through the pipeline (only possible on abrupt thread death).
        Collection<CompletableFuture<AppendResult.CallbackResult>> leftovers = slidingWindowService != null ?
            slidingWindowService.drainPendingFutures() : Collections.emptyList();
        for (CompletableFuture<AppendResult.CallbackResult> f : leftovers) {
            f.completeExceptionally(new WALShutdownException("file-wal shutting down"));
        }

        // Step 3: stop the header-flusher; in-flight trim tasks may still delete segments.
        shutdownExecutor(walHeaderFlushExecutor);

        // Step 4: persist final metadata and close data segments.
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
        long recordOffset;

        Lock lock = slidingWindowService.getBlockLock();
        lock.lock();
        try {
            Block block = slidingWindowService.getCurrentBlockLocked();
            Block.RecordSupplier supplier = (offset, header) -> WALUtil.generateRecord(body, header, crc, offset);
            recordOffset = block.addRecord(recordSize, supplier, appendResultFuture);
            if (recordOffset < 0) {
                // current block is full - seal it and start a new one
                // This may throw OverCapacityException if the pending blocks queue is full,
                // providing back-pressure to the caller.
                block = slidingWindowService.sealAndNewBlockLocked(block);
                recordOffset = block.addRecord(recordSize, supplier, appendResultFuture);
            }
        } finally {
            lock.unlock();
        }
        CompletableFuture<AppendResult.CallbackResult> timedFuture =
            FutureUtil.timeoutWithNewReturn(appendResultFuture, timeoutSeconds, TimeUnit.SECONDS,
                () -> failed.set(true));
        final AppendResult appendResult = new AppendResultImpl(recordOffset, timedFuture);
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
        CompletableFuture<Void> cf = trim(newStartOffset, true).thenRun(() -> {
            slidingWindowService.resetTo(newStartOffset);
            segmentManager.clearCurrentSegment();
            resetFinished.set(true);
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
        private long segmentRollThresholdBytes = 512L << 20;
        // Max write throughput in bytes/s. Long.MAX_VALUE means unlimited.
        private long writeBandwidthLimit = Long.MAX_VALUE;
        // Max bytes per in-memory block before a new block is created.
        private long blockMaxSize = 20L << 20; // 20 MiB
        // Soft size limit per block (block can exceed it only with a single oversized record).
        private long blockSoftLimit = 4L << 20;  // 4 MiB
        // wal io request limit
        private int writeRateLimit = 3000;
        private int fsyncRateLimit = 100;
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

        public FilesystemWALServiceBuilder writeRateLimit(int writeRateLimit) {
            this.writeRateLimit = writeRateLimit;
            return this;
        }

        public FilesystemWALServiceBuilder fsyncRateLimit(int fsyncRateLimit) {
            this.fsyncRateLimit = fsyncRateLimit;
            return this;
        }

        public FilesystemWALServiceBuilder writeBandwidthLimit(long writeBandwidthLimit) {
            if (writeBandwidthLimit <= 0) {
                throw new IllegalArgumentException("writeBandwidthLimit must be positive: " + writeBandwidthLimit);
            }
            this.writeBandwidthLimit = writeBandwidthLimit;
            return this;
        }

        /**
         * Hard size limit (bytes) for the in-memory append block. Bigger values let more records
         * coalesce per block, smaller values trade memory for latency.
         */
        public FilesystemWALServiceBuilder blockMaxSize(long blockMaxSize) {
            if (blockMaxSize <= 0) {
                throw new IllegalArgumentException("blockMaxSize must be positive: " + blockMaxSize);
            }
            this.blockMaxSize = blockMaxSize;
            return this;
        }

        /**
         * Soft size limit (bytes) for the in-memory append block. Records that would push a non-empty
         * block past this limit force the block to be sealed first.
         */
        public FilesystemWALServiceBuilder blockSoftLimit(long blockSoftLimit) {
            if (blockSoftLimit <= 0) {
                throw new IllegalArgumentException("blockSoftLimit must be positive: " + blockSoftLimit);
            }
            this.blockSoftLimit = blockSoftLimit;
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
            if (blockSoftLimit > blockMaxSize) {
                blockSoftLimit = blockMaxSize;
            }

            FilesystemWALService service = new FilesystemWALService();
            service.directory = new File(directoryPath);
            service.walMetadataFile = new WalMetadataFile(service.directory);
            service.segmentManager = new SegmentManager(service.directory);
            service.segmentRollThresholdBytes = segmentRollThresholdBytes;
            service.timeoutSeconds = timeoutSeconds;
            service.recoveryMode = recoveryMode;

            service.writeBandwidthLimit = writeBandwidthLimit;
            service.writeRateLimit = writeRateLimit;
            service.fsyncRateLimit = fsyncRateLimit;
            service.blockMaxSize = blockMaxSize;
            service.blockSoftLimit = blockSoftLimit;
            service.slidingWindowService = new FilesystemSlidingWindowService(blockMaxSize, blockSoftLimit);
            service.writeRateBucket = buildBucket(writeRateLimit);
            long writeBandwidthLimitByBlock = writeBandwidthLimit == Long.MAX_VALUE ? Long.MAX_VALUE :
                writeBandwidthLimit;
            service.writeBandwidthBucket = buildBlockingBucket(writeBandwidthLimitByBlock);
            service.fsyncRateBucket = buildBucket(fsyncRateLimit);
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

    /**
     * A block that has been sealed by the block-poll thread, picked up by the write thread and is
     * ready to be written into its destination segment.
     */
    private static final class WriteRequest {
        private final Block block;
        private final long startOffset;
        private final long endOffset;

        WriteRequest(Block block) {
            this.block = block;
            this.startOffset = block.startOffset();
            this.endOffset = block.startOffset() + block.size();
        }
    }

    /**
     * A batch of {@link WriteRequest}s whose payload has been handed to the OS and is waiting for
     * fsync.
     */
    private static final class ForceWriteRequest {
        private final Segment segment;
        private final long startOffset;
        private final long endOffset;
        private final List<Block> blocks;
        private final boolean shouldClose;

        private ForceWriteRequest(Segment segment, long startOffset, long endOffset, List<Block> blocks,
                                  boolean shouldClose) {
            this.segment = segment;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.blocks = blocks;
            this.shouldClose = shouldClose;
        }

        @Override
        public String toString() {
            return "ForceWriteRequest{" + "segment=" + segment + ", endOffset=" + endOffset + ", blocks=" + blocks +
                ", shouldClose=" + shouldClose + '}';
        }
    }

    /**
     * Single-threaded write stage: consumes {@link WriteRequest}s, writes each block to its
     * segment (no fsync) and hands off complete batches to the force-write thread.
     */
    private final class WriteLoop implements Runnable {
        private final List<Block> batchBlocks = new ArrayList<>();
        private Segment batchSegment = null;
        private long batchStartOffset = -1;
        private long batchEndOffset = -1;
        private int batchEntries = 0;
        private long segmentSize = 0;
        private long lastFsyncTime = System.currentTimeMillis();


        @Override
        public void run() {
            LOGGER.info("file-wal write thread started");
            try {
                while (started.get() || !writeExecutor.isTerminated()) {
                    if (writeRateBucket != null) {
                        writeRateBucket.asBlocking().consume(1);
                    }
                    Block block = slidingWindowService.pollBlock();
                    if (block != null) {
                        block.polled();
                        write(block);
                    }
                    maybeFlushBatch();
                }
            } catch (Throwable t) {
                started.set(false);
                LOGGER.error("file-wal write thread terminated unexpectedly", t);
            } finally {
                LOGGER.info("file-wal write thread exited");
            }
        }

        private void write(Block block) {
            try {
                Segment segment = segmentManager.latestSegment(block.startOffset());
                ByteBuf data = block.data();
                if (writeBandwidthBucket != null) {
                    writeBandwidthBucket.consumeUninterruptibly(data.readableBytes());
                }
                segment.write(data);
                segmentSize += block.size();
                updateFsyncBatch(segment, block);
                boolean shouldSegmentClose = segmentSize > segmentRollThresholdBytes;
                if (shouldSegmentClose) {
                    flushBatchToForceWrite(true);
                    segmentManager.rollOverSegment(block.endOffset());
                    segmentSize = 0;
                }
            } catch (Exception e) {
                LOGGER.error("failed to write block, walOffset: {}", block.startOffset(), e);
                FutureUtil.completeExceptionally(block.futures().iterator(), e);
                block.release();
            }
        }

        private void updateFsyncBatch(Segment segment, Block block) {
            batchSegment = segment;
            if (!hasOpenBatch()) {
                batchStartOffset = block.startOffset();
            }
            batchEndOffset = block.endOffset();
            batchEntries++;
            batchBlocks.add(block);
        }

        private void maybeFlushBatch() {
            if (!hasOpenBatch()) {
                return;
            }
            if (System.currentTimeMillis() - lastFsyncTime < 2) {
                return;
            }
            if (fsyncRateBucket != null && !fsyncRateBucket.tryConsume(1)) {
                return;
            }
            flushBatchToForceWrite(false);
        }

        private boolean hasOpenBatch() {
            return batchEntries > 0;
        }

        private void flushBatchToForceWrite(boolean shouldClose) {
            ForceWriteRequest req = new ForceWriteRequest(batchSegment, batchStartOffset, batchEndOffset,
                new ArrayList<>(batchBlocks), shouldClose);
            try {
                forceQueue.put(req);
                lastFsyncTime = System.currentTimeMillis();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                WALShutdownException ex = new WALShutdownException("interrupted while enqueuing force-write request");
                for (Block block : req.blocks) {
                    callbackExecutor.execute(() -> {
                        FutureUtil.completeExceptionally(block.futures().iterator(), ex);
                        block.release();
                    });
                }
            }
            resetOpenBatch();
        }

        private void resetOpenBatch() {
            batchBlocks.clear();
            batchSegment = null;
            batchStartOffset = -1;
            batchEndOffset = -1;
            batchEntries = 0;
        }
    }

    /**
     * Single-threaded force-write stage: groups requests, fsyncs each distinct segment exactly
     * once per batch, completes pending futures and releases the associated buffers.
     * <p>
     * The loop continues while {@code started} is true <em>or</em> while {@link #writeExecutor} is still
     * running.  This is necessary because {@code WriteLoop#drainOnShutdown()} pushes a final
     * batch to {@code forceQueue} after {@code started} is set to {@code false}; if this loop
     * exited as soon as {@code started} became false it would miss that final batch and leave
     * the associated futures unresolved.  Once {@link #writeExecutor} has terminated no new items
     * can be added, so a final non-blocking drain in the {@code finally} block catches anything
     * that arrived in the window between the last timed poll and the loop exit.
     */
    private final class ForceWriteLoop implements Runnable {

        @Override
        public void run() {
            LOGGER.info("file-wal force-write thread started");
            ForceWriteRequest[] batch = new ForceWriteRequest[FSYNC_COALESCE_BUFFER_CAPACITY];
            try {
                while (started.get() || !writeExecutor.isTerminated()) {
                    try {
                        int count = forceQueue.pollAll(batch, 1, TimeUnit.MILLISECONDS);
                        if (count != 0) {
                            processForceWriteBatch(batch, count);
                        } else {
                            TimeUnit.MILLISECONDS.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Throwable t) {
                        started.set(false);
                        LOGGER.error("unexpected error in force-write thread", t);
                        break;
                    }
                }
            } finally {
                // Drain any items deposited by WriteLoop.drainOnShutdown() after the main loop
                // exited. writeExecutor has terminated (or we broke out on error) so no further
                // items can be added.
                drainOnShutdown(batch);
                LOGGER.info("file-wal force-write thread exited");
            }
        }

        private void drainOnShutdown(ForceWriteRequest[] batch) {
            while (true) {
                int count = 0;
                ForceWriteRequest req;
                while (count < batch.length && (req = forceQueue.poll()) != null) {
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
                ForceWriteRequest last = requests.get(requests.size() - 1);
                boolean closeSegment = requests.stream().anyMatch(r -> r.shouldClose);
                try {
                    final long writeStart = System.nanoTime();
                    segment.fsync();
                    StorageOperationStats.getInstance().appendWALWriteStats.record(TimerUtil.timeElapsedSince(writeStart, TimeUnit.NANOSECONDS));
                    // Capture the durable end offset at the moment of the fsync so that
                    // flushedOffset() always returns a stable, already-synced value.
                    flushedMarkOffset.set(last.endOffset);
                    for (ForceWriteRequest req : requests) {
                        for (Block block : req.blocks) {
                            final long callbackOffset = block.endOffset();
                            callbackExecutor.execute(() -> {
                                final long startTime = System.nanoTime();
                                FutureUtil.complete(block.futures().iterator(), new AppendResult.CallbackResult() {
                                    @Override
                                    public long flushedOffset() {
                                        return callbackOffset;
                                    }

                                    @Override
                                    public String toString() {
                                        return "CallbackResult{" + "flushedOffset=" + flushedOffset() + '}';
                                    }
                                });
                                StorageOperationStats.getInstance().appendWALAfterStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
                            });
                        }
                    }
                } catch (Throwable t) {
                    for (ForceWriteRequest req : requests) {
                        for (Block block : req.blocks) {
                            FutureUtil.completeExceptionally(block.futures().iterator(), t);
                            block.release();
                        }
                    }
                    LOGGER.error("unexpected error in force-write thread", t);
                } finally {
                    requests.forEach(req -> {
                        req.blocks.forEach(Block::release);
                    });
                    if (closeSegment) {
                        segment.close();
                    }
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
                    if (!firstInvalidSeen) {
                        firstInvalidSeen = true;
                        firstInvalidOffset = nextRecoverOffset;
                        LOGGER.info("first invalid offset met during recovery, offset {}, detail: '{}'",
                            firstInvalidOffset, e.getMessage());
                    }
                    // FilesystemWAL stores records sequentially with no block-alignment gaps.
                    // A read failure means the rest of the current segment is invalid (the write
                    // was interrupted). Jump directly to the next segment's start offset; the
                    // block-aligned jump used by BlockWAL is incorrect here and would skip valid
                    // records packed at the beginning of the next segment.
                    Segment nextSeg = segmentManager.nextSegmentAfter(nextRecoverOffset);
                    if (nextSeg == null) {
                        return false;
                    }
                    nextRecoverOffset = nextSeg.startOffset();
                } catch (IOException e) {
                    LOGGER.error("failed to read record at offset {}", nextRecoverOffset, e);
                    throw new RuntimeIOException(e);
                }
            }
            return false;
        }

        private boolean shouldContinue() {
            // FilesystemWAL stores records in rolling segment files with no circular wrap-around.
            // Continue recovery as long as there are segments to read; the BlockWAL-style slack
            // window does not apply here because valid records in a later segment must not be
            // silently skipped just because it starts far from the first invalid offset.
            return segmentManager.segmentForOffset(nextRecoverOffset) != null
                || segmentManager.nextSegmentAfter(nextRecoverOffset) != null;
        }
    }

}
