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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the set of WAL segment files inside a directory. Each segment is an append-only file
 * that covers a contiguous range of global record offsets. The manager hands out the right
 * {@link Segment} for a given global record offset, creates new segments when needed, and removes
 * segments that have been fully trimmed.
 */
public class SegmentManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentManager.class);

    private final File directory;
    private final int segmentWriteBufferBytes;

    private static final int MAX_BACKUP_JOURNALS = 2;

    private Segment currentSegment;

    /**
     * All known segments, keyed by start offset. Sorted in ascending order. Access must be
     * synchronized via {@link #segmentsLock}.
     */
    private final NavigableMap<Long, Segment> segments = new TreeMap<>();
    private final ReentrantLock segmentsLock = new ReentrantLock();

    public SegmentManager(File directory, int segmentWriteBufferBytes) {
        if (segmentWriteBufferBytes <= 0) {
            throw new IllegalArgumentException("segmentWriteBufferBytes must be positive: " + segmentWriteBufferBytes);
        }
        this.directory = directory;
        this.segmentWriteBufferBytes = segmentWriteBufferBytes;
    }

    /**
     * Scan the directory and open every existing segment file. Should be called once during startup.
     */
    public void load() throws IOException {
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new IOException("mkdirs " + directory + " failed");
            }
        }
        if (!directory.isDirectory()) {
            throw new IOException(directory + " is not a directory");
        }
        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }
        segmentsLock.lock();
        try {
            for (File file : files) {
                long startOffset = Segment.parseStartOffset(file.getName());
                if (startOffset < 0) {
                    continue;
                }
                Segment segment = new Segment(file, startOffset, segmentWriteBufferBytes);
                Segment prev = segments.put(segment.startOffset(), segment);
                if (prev != null) {
                    prev.close();
                }
            }
        } finally {
            segmentsLock.unlock();
        }
        LOGGER.info("loaded {} WAL segment(s) from {}", segments.size(), directory);
    }

    /**
     * Returns whether the directory has any segments yet.
     */
    public boolean isEmpty() {
        segmentsLock.lock();
        try {
            return segments.isEmpty();
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Snapshot of all segments ordered by start offset (read-only view).
     */
    public List<Segment> snapshot() {
        segmentsLock.lock();
        try {
            return new ArrayList<>(segments.values());
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Find the segment whose offset range covers {@code walLogicalOffset}. Returns {@code null} if no
     * such segment is currently loaded.
     */
    public Segment segmentForOffset(long walLogicalOffset) {
        segmentsLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.floorEntry(walLogicalOffset);
            if (entry == null) {
                return null;
            }
            Segment segment = entry.getValue();
            return segment.containsOffset(walLogicalOffset) ? segment : null;
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Find the smallest segment whose start offset is greater than {@code walOffset}. Useful
     * during recovery when a hole in the segment range needs to be skipped (e.g. earlier segments
     * have been trimmed and deleted).
     */
    public Segment nextSegmentAfter(long walOffset) {
        segmentsLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.higherEntry(walOffset);
            return entry == null ? null : entry.getValue();
        } finally {
            segmentsLock.unlock();
        }
    }

    public void rollOverSegment(long walOffset) throws IOException {
        segmentsLock.lock();
        try {
            currentSegment = createNewSegment(walOffset);
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Create a new segment starting at the given offset. Must be called while holding {@link #segmentsLock}.
     */
    private Segment createNewSegment(long walOffset) throws IOException {
        File file = new File(directory, Segment.buildFileName(walOffset));
        Segment segment = new Segment(file, walOffset, segmentWriteBufferBytes);
        segments.put(segment.startOffset(), segment);
        LOGGER.info("created new WAL segment {}", segment);
        return segment;
    }

    /**
     * Returns the most recently created segment by start offset.
     */
    public Segment latestSegment(long startOffset) throws IOException {
        segmentsLock.lock();
        try {
            if (currentSegment != null && segments.get(currentSegment.startOffset()) != currentSegment) {
                currentSegment = null;
            }
            if (currentSegment == null) {
                Segment existing = segments.get(startOffset);
                currentSegment = existing != null ? existing : createNewSegment(startOffset);
            }
            return currentSegment;
        } finally {
            segmentsLock.unlock();
        }
    }

    public void clearCurrentSegment() {
        segmentsLock.lock();
        try {
            currentSegment = null;
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Returns the highest WAL logical end offset reachable from the currently loaded segments
     * (i.e. the exclusive end offset of the latest segment, or 0 if no segment exists yet).
     */
    public long highestKnownEndOffset() {
        segmentsLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.lastEntry();
            return entry == null ? 0 : entry.getValue().endOffsetExclusive();
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Delete every segment whose end offset is less than or equal to {@code trimOffset + 1}. In
     * other words, segments that contain no offset greater than {@code trimOffset}.
     */
    public int deleteSegmentsBelowOrEqual(long trimOffset) {
        if (trimOffset < 0 || segments.size() < MAX_BACKUP_JOURNALS) {
            return 0;
        }
        int deleted = 0;
        long deleteBeforeOffset = trimOffset == Long.MAX_VALUE ? Long.MAX_VALUE : trimOffset + 1;
        segmentsLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.firstEntry();
            while (entry != null) {
                Map.Entry<Long, Segment> nextEntry = segments.higherEntry(entry.getKey());
                if (nextEntry == null || nextEntry.getKey() > deleteBeforeOffset) {
                    break;
                }
                Segment segment = entry.getValue();
                segments.remove(entry.getKey());
                if (currentSegment == segment) {
                    currentSegment = null;
                }
                segment.deleteQuietly();
                LOGGER.info("trimmed segment {}", segment);
                deleted++;
                entry = nextEntry;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            segmentsLock.unlock();
        }
        return deleted;
    }

    /**
     * Close every open segment.
     */
    public void close() {
        segmentsLock.lock();
        try {
            for (Segment segment : segments.values()) {
                segment.close();
            }
            segments.clear();
            currentSegment = null;
        } finally {
            segmentsLock.unlock();
        }
    }
}
