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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class WalMetadataFileTest {

    @Test
    void writeAndReadHeader() throws IOException {
        Path dir = Files.createTempDirectory("wal-meta-test-");
        try {
            WalMetadataFile meta = new WalMetadataFile(dir.toFile());
            meta.openOrCreate();
            assertTrue(new File(dir.toFile(), WalMetadataFile.FILE_NAME).isFile());

            FilesystemWALHeader header = new FilesystemWALHeader(0);
            header.updateTrimOffset(42);
            meta.writeHeader(header);

            WalMetadataFile meta2 = new WalMetadataFile(dir.toFile());
            meta2.openOrCreate();
            FilesystemWALHeader recovered = meta2.readLatestHeader();
            assertNotNull(recovered);
            assertEquals(42, recovered.getTrimOffset());
            meta2.close();
            meta.close();
        } finally {
            deleteRecursive(dir.toFile());
        }
    }

    private static void deleteRecursive(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        file.delete();
    }
}
