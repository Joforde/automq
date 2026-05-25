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
import com.automq.stream.s3.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
class SegmentTest {

    @Test
    void appendAndReadBack() throws IOException {
        String path = TestUtils.tempFilePath();
        File file = new File(path);
        int dataLength = 10;
        long startOffset = 0;
        Segment segment = new Segment(file, startOffset);
        try {
            List<byte[]> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                ByteBuf data = TestUtils.random(dataLength);
                byte[] copy = new byte[dataLength];
                data.getBytes(data.readerIndex(), copy);
                expected.add(copy);
                segment.write(data);
            }
            segment.fsync();

            for (int i = 0; i < 10; i++) {
                ByteBuf readBuf = ByteBufAlloc.byteBuffer(dataLength);
                try {
                    int n = segment.read(readBuf, dataLength);
                    assertEquals(dataLength, n);
                    byte[] actual = ByteBufUtil.getBytes(readBuf, readBuf.readerIndex(), readBuf.readableBytes());
                    assertArrayEquals(expected.get(i), actual);
                } finally {
                    readBuf.release();
                }
            }
        } finally {
            segment.close();
            file.delete();
        }
    }

    @Test
    void appendReadAfterReopen() throws IOException {
        String path = TestUtils.tempFilePath();
        File file = new File(path);
        long startOffset = 4096;
        int dataLength = 32;
        byte[] expected;

        Segment segment = new Segment(file, startOffset);
        try {
            ByteBuf data = TestUtils.random(dataLength);
            expected = new byte[dataLength];
            data.getBytes(data.readerIndex(), expected);
            segment.write(data);
            segment.fsync();
            segment.close();

            segment = new Segment(file, startOffset);

            ByteBuf readBuf = ByteBufAlloc.byteBuffer(dataLength);
            try {
                assertEquals(dataLength, segment.read(readBuf, dataLength));
                assertArrayEquals(expected, ByteBufUtil.getBytes(readBuf));
            } finally {
                readBuf.release();
            }
        } finally {
            segment.deleteQuietly();
        }
    }
}
