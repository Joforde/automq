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
    void writeAtAndReadBack() throws IOException {
        String path = TestUtils.tempFilePath();
        File file = new File(path);
        int dataLength = 10;
        long startOffset = 1024;
        Segment segment = new Segment(file, startOffset);
        try {
            segment.openOrCreate();

            List<byte[]> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                ByteBuf data = TestUtils.random(dataLength);
                byte[] copy = new byte[dataLength];
                data.getBytes(data.readerIndex(), copy);
                expected.add(copy);
                segment.writeAt(data, startOffset + (long) i * dataLength);
            }
            segment.fsync();

            for (int i = 0; i < 10; i++) {
                ByteBuf readBuf = ByteBufAlloc.byteBuffer(dataLength);
                try {
                    int n = segment.readAt(readBuf, startOffset + (long) i * dataLength, dataLength);
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
    void writeReadAfterReopen() throws IOException {
        String path = TestUtils.tempFilePath();
        File file = new File(path);
        long startOffset = 4096;
        int dataLength = 32;
        byte[] expected;

        Segment segment = new Segment(file, startOffset);
        try {
            segment.openOrCreate();

            ByteBuf data = TestUtils.random(dataLength);
            expected = new byte[dataLength];
            data.getBytes(data.readerIndex(), expected);
            segment.writeAt(data, startOffset);
            segment.fsync();
            segment.close();

            segment = new Segment(file, startOffset);
            segment.openOrCreate();

            ByteBuf readBuf = ByteBufAlloc.byteBuffer(dataLength);
            try {
                assertEquals(dataLength, segment.readAt(readBuf, startOffset, dataLength));
                assertArrayEquals(expected, ByteBufUtil.getBytes(readBuf));
            } finally {
                readBuf.release();
            }
        } finally {
            segment.deleteQuietly();
        }
    }
}
