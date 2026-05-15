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

package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.impl.filesystem.FilesystemWALService;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;
import static com.automq.stream.s3.wal.benchmark.BenchTool.prepareWalPath;

/**
 * RecoveryBench benchmarks recovery time for a {@link WriteAheadLog} implementation
 * ({@link BlockWALService} or {@link FilesystemWALService}).
 */
public class RecoveryBench implements AutoCloseable {

    private final WriteAheadLog writeWal;
    private Random random = new Random();

    public RecoveryBench(Config config) throws IOException {
        this.writeWal = newWriteWal(config);
        this.writeWal.start();
        recoverAndReset(writeWal);
    }

    private static WriteAheadLog newWriteWal(Config config) throws IOException {
        switch (config.walKind) {
            case BLOCK:
                return BlockWALService.builder(config.path, config.capacity).build();
            case FILESYSTEM:
                FilesystemWALService.FilesystemWALServiceBuilder b = FilesystemWALService.builder(config.path);
                if (config.segmentRollThresholdBytes != null) {
                    b.segmentRollThresholdBytes(config.segmentRollThresholdBytes);
                }
                return b.build();
            default:
                throw new IllegalStateException("Unhandled WAL kind: " + config.walKind);
        }
    }

    private static WriteAheadLog newRecoveryWal(Config config) throws IOException {
        switch (config.walKind) {
            case BLOCK:
                return BlockWALService.recoveryBuilder(config.path).build();
            case FILESYSTEM:
                FilesystemWALService.FilesystemWALServiceBuilder b = FilesystemWALService.recoveryBuilder(config.path);
                if (config.segmentRollThresholdBytes != null) {
                    b.segmentRollThresholdBytes(config.segmentRollThresholdBytes);
                }
                return b.build();
            default:
                throw new IllegalStateException("Unhandled WAL kind: " + config.walKind);
        }
    }

    private static int recoverAndReset(WriteAheadLog wal) {
        int recovered = 0;
        for (Iterator<RecoverResult> it = wal.recover(); it.hasNext(); ) {
            it.next().record().release();
            recovered++;
        }
        wal.reset().join();
        return recovered;
    }

    public static void main(String[] args) throws Exception {
        Namespace ns = parseArgs(Config.parser(), args);
        Config config = new Config(ns);

        prepareWalPath(config.walKind, config.path);
        try (RecoveryBench bench = new RecoveryBench(config)) {
            bench.run(config);
        }
    }

    private void run(Config config) throws Exception {
        writeRecords(config.numRecords, config.recordSizeBytes);
        writeWal.shutdownGracefully();
        recoverRecords(config);
    }

    private void writeRecords(int numRecords, int recordSizeBytes) throws OverCapacityException {
        System.out.println("Writing " + numRecords + " records of size " + recordSizeBytes + " bytes");
        byte[] bytes = new byte[recordSizeBytes];
        random.nextBytes(bytes);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes).retain();

        List<CompletableFuture<AppendResult.CallbackResult>> pending = new ArrayList<>(numRecords);
        try {
            for (int i = 0; i < numRecords; i++) {
                AppendResult result = writeWal.append(payload.retainedDuplicate());
                pending.add(result.future());
            }
            CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
            System.out.println("Appended " + numRecords + " records (all futures completed)");
        } finally {
            payload.release();
        }
    }

    private void recoverRecords(Config config) throws IOException {
        WriteAheadLog recoveryLog = newRecoveryWal(config);
        recoveryLog.start();
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            int recovered = recoverAndReset(recoveryLog);
            System.out.println("Recovered " + recovered + " records in " + stopWatch.getTime() + " ms");
        } finally {
            recoveryLog.shutdownGracefully();
        }
    }

    @Override
    public void close() {
        writeWal.shutdownGracefully();
    }

    static class Config {
        final WalBenchmarkKind walKind;
        // following fields are WAL configuration
        final String path;
        final Long capacity;
        final Long segmentRollThresholdBytes;

        // following fields are benchmark configuration
        final Integer numRecords;
        final Integer recordSizeBytes;

        Config(Namespace ns) {
            this.walKind = WalBenchmarkKind.fromCli(ns.getString("wal"));
            this.path = ns.getString("path");
            this.capacity = ns.getLong("capacity");
            this.segmentRollThresholdBytes = ns.getLong("segmentRollBytes");
            this.numRecords = ns.getInt("records");
            this.recordSizeBytes = ns.getInt("recordSize");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                .newArgumentParser("RecoveryBench")
                .defaultHelp(true)
                .description("Benchmark recovery performance of BlockWALService or FilesystemWALService");
            parser.addArgument("--wal")
                .choices("block", "filesystem")
                .setDefault("block")
                .help("WAL implementation to benchmark");
            parser.addArgument("-p", "--path")
                .required(true)
                .help("Block WAL: single file (or block device) path. Filesystem WAL: directory for segment files");
            parser.addArgument("-c", "--capacity")
                .type(Long.class)
                .setDefault((long) 3 << 30)
                .help("Block WAL only: capacity of the WAL in bytes (ignored for filesystem WAL)");
            parser.addArgument("--segment-roll-bytes")
                .dest("segmentRollBytes")
                .type(Long.class)
                .help("Filesystem WAL only: roll to a new segment after this many record bytes per file (default: implementation default, typically 1 GiB)");
            parser.addArgument("--records")
                .type(Integer.class)
                .setDefault(1 << 20)
                .help("number of records to write");
            parser.addArgument("--record-size")
                .dest("recordSize")
                .type(Integer.class)
                .setDefault(1 << 10)
                .help("size of each record in bytes");
            return parser;
        }
    }
}
