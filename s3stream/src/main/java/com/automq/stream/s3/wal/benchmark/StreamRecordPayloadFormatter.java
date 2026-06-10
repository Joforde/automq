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

import com.automq.stream.s3.model.StreamRecordBatch;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Formats {@link StreamRecordBatch} payload for WAL recovery tools.
 */
public final class StreamRecordPayloadFormatter {
    private static final int MAX_TEXT_BYTES = 512;
    private static final int MAX_HEX_BYTES = 128;
    private static final byte META_MAGIC_V0 = 0;

    private StreamRecordPayloadFormatter() {
    }

    public static String format(StreamRecordBatch batch) {
        StringBuilder sb = new StringBuilder();
        sb.append(batch);
        String payload = formatPayload(batch.getPayload());
        if (!payload.isEmpty()) {
            sb.append(", payload=").append(payload);
        }
        return sb.toString();
    }

    static String formatPayload(ByteBuf payload) {
        if (payload == null || !payload.isReadable()) {
            return "";
        }
        ByteBuffer buffer = payload.nioBuffer(payload.readerIndex(), payload.readableBytes());
        String meta = tryFormatMetaKeyValue(buffer.duplicate());
        if (meta != null) {
            return meta;
        }
        return tryFormatKafkaRecords(buffer.duplicate());
    }

    private static String tryFormatMetaKeyValue(ByteBuffer buffer) {
        if (buffer.remaining() < 5) {
            return null;
        }
        byte magic = buffer.get(buffer.position());
        if (magic != META_MAGIC_V0) {
            return null;
        }
        try {
            buffer.get(); // magic
            int keyLength = buffer.getInt();
            if (keyLength < 0 || keyLength > buffer.remaining()) {
                return null;
            }
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            byte[] valueBytes = new byte[buffer.remaining()];
            buffer.get(valueBytes);
            return "MetaKeyValue{key=" + key + ", value=" + formatBytes(valueBytes) + '}';
        } catch (Exception e) {
            return null;
        }
    }

    private static String tryFormatKafkaRecords(ByteBuffer buffer) {
        if (buffer.remaining() == 0) {
            return "";
        }
        try {
            MemoryRecords records = MemoryRecords.readableRecords(buffer);
            StringBuilder sb = new StringBuilder("messages=[");
            boolean firstBatch = true;
            for (RecordBatch batch : records.batches()) {
                if (!firstBatch) {
                    sb.append(", ");
                }
                firstBatch = false;
                sb.append("batch{baseOffset=").append(batch.baseOffset())
                    .append(", lastOffset=").append(batch.lastOffset())
                    .append(", compression=").append(batch.compressionType())
                    .append(", records=[");
                boolean firstRecord = true;
                for (Record record : batch) {
                    if (!firstRecord) {
                        sb.append(", ");
                    }
                    firstRecord = false;
                    sb.append("record{offset=").append(record.offset())
                        .append(", timestamp=").append(record.timestamp())
                        .append(", key=").append(formatNullableBuffer(record.key()))
                        .append(", value=").append(formatNullableBuffer(record.value()))
                        .append('}');
                }
                sb.append("]}");
            }
            sb.append(']');
            return sb.toString();
        } catch (Exception e) {
            byte[] raw = new byte[buffer.remaining()];
            buffer.get(raw);
            return "raw=" + formatBytes(raw) + " (decode failed: " + e.getMessage() + ')';
        }
    }

    private static String formatNullableBuffer(ByteBuffer buf) {
        if (buf == null) {
            return "null";
        }
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return formatBytes(bytes);
    }

    private static String formatBytes(byte[] bytes) {
        if (bytes.length == 0) {
            return "\"\"";
        }
        if (isPrintableUtf8(bytes)) {
            String text = new String(bytes, StandardCharsets.UTF_8);
            if (text.length() > MAX_TEXT_BYTES) {
                return '"' + text.substring(0, MAX_TEXT_BYTES) + "...\"(" + bytes.length + " bytes)";
            }
            return '"' + text + '"';
        }
        int len = Math.min(bytes.length, MAX_HEX_BYTES);
        String hex = bytesToHex(bytes, len);
        if (bytes.length > MAX_HEX_BYTES) {
            return "hex:" + hex + "...(" + bytes.length + " bytes)";
        }
        return "hex:" + hex;
    }

    private static boolean isPrintableUtf8(byte[] bytes) {
        try {
            String text = new String(bytes, StandardCharsets.UTF_8);
            if (text.isEmpty()) {
                return true;
            }
            for (int i = 0; i < text.length(); i++) {
                char ch = text.charAt(i);
                if (ch == '\n' || ch == '\r' || ch == '\t') {
                    continue;
                }
                if (Character.isISOControl(ch)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String bytesToHex(byte[] bytes, int len) {
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }
        return sb.toString();
    }
}
