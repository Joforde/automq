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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.exception.RuntimeIOException;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.util.WALChannel;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.wal.util.WALUtil.isBlockDevice;

public class BenchTool {

    public static Namespace parseArgs(ArgumentParser parser, String[] args) {
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return ns;
    }

    /**
     * Prepare on-disk state before a benchmark run, according to the WAL kind.
     * <p>
     * {@link WalBenchmarkKind#BLOCK}: same as {@link #resetWALHeader(String)} (single file or block device).<br>
     * {@link WalBenchmarkKind#FILESYSTEM}: remove any previous WAL directory (or file at the path) and recreate an empty directory.
     */
    public static void prepareWalPath(WalBenchmarkKind kind, String path) throws IOException {
        switch (kind) {
            case BLOCK:
                resetWALHeader(path);
                break;
            case FILESYSTEM:
                if (isBlockDevice(path)) {
                    throw new IOException("Filesystem WAL benchmarks expect a directory path, not a block device: " + path);
                }
                resetFilesystemWalDirectory(path);
                break;
            default:
                throw new IllegalStateException("Unhandled WAL kind: " + kind);
        }
    }

    private static void resetFilesystemWalDirectory(String path) throws IOException {
        System.out.println("Resetting filesystem WAL directory: " + path);
        Path root = Path.of(path);
        if (Files.exists(root)) {
            if (Files.isRegularFile(root)) {
                Files.delete(root);
            } else if (Files.isDirectory(root)) {
                try (Stream<Path> walk = Files.walk(root)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            throw new RuntimeIOException(e);
                        }
                    });
                }
            } else {
                throw new IOException("Path exists but is neither a regular file nor a directory: " + root);
            }
        }
        Files.createDirectories(root);
    }

    public static void resetWALHeader(String path) throws IOException {
        System.out.println("Resetting WAL header");
        if (isBlockDevice(path)) {
            // block device
            int capacity = BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
            WALChannel channel = WALChannel.builder(path).capacity(capacity).build();
            channel.open();
            ByteBuf buf = ByteBufAlloc.byteBuffer(capacity);
            buf.writeZero(capacity);
            channel.write(buf, 0);
            buf.release();
            channel.close();
        } else {
            // normal file
            File file = new File(path);
            if (file.isFile() && !file.delete()) {
                throw new IOException("Failed to delete existing file " + file);
            }
        }
    }
}
