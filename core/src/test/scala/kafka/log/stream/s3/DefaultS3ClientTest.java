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

package kafka.log.stream.s3;

import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.impl.filesystem.FilesystemWALService;
import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class DefaultS3ClientTest {

    @Test
    public void buildRecoveryWALShouldUseBlockImplementationForFileProtocol() {
        IdURI uri = IdURI.parse("0@file:///tmp/block-wal");
        WriteAheadLog wal = DefaultS3Client.buildRecoveryWAL(uri, "/tmp/recovery-wal");
        assertInstanceOf(BlockWALService.class, wal);
    }

    @Test
    public void buildRecoveryWALShouldUseFilesystemImplementationForFilesystemProtocol() {
        IdURI uri = IdURI.parse("0@filesystem:///tmp/filesystem-wal");
        WriteAheadLog wal = DefaultS3Client.buildRecoveryWAL(uri, "/tmp/recovery-wal-dir");
        assertInstanceOf(FilesystemWALService.class, wal);
    }

    @Test
    public void buildRecoveryWALShouldFallbackToUriPathWhenDevicePathIsEmpty() {
        IdURI uri = IdURI.parse("0@filesystem:///tmp/filesystem-wal");
        WriteAheadLog wal = DefaultS3Client.buildRecoveryWAL(uri, "");
        assertInstanceOf(FilesystemWALService.class, wal);
    }
}
