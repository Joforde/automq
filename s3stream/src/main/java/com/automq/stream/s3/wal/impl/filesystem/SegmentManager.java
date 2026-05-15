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

import com.automq.stream.s3.wal.util.WALUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the set of WAL segment files inside a directory. Hands out the right {@link Segment} for a
 * given global record offset, handles rollover when the active segment would overflow, and removes
 * segments that have been fully trimmed.
 */
class SegmentManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentManager.class);

    private final File directory;
    private final long maxSegmentSize;
    private final long maxRecordBytesPerSegment;

    /**
     * All known segments, keyed by start offset. Sorted in ascending order. Access must be
     * synchronized via {@link #lock}.
     */
    private final NavigableMap<Long, Segment> segments = new TreeMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile SegmentInitializer initializer;

    SegmentManager(File directory, long maxSegmentSize) {
        assert maxSegmentSize > FileSystemWALService.WAL_HEADER_TOTAL_CAPACITY;
        assert WALUtil.isAligned(maxSegmentSize) : "maxSegmentSize must be block aligned";
        this.directory = directory;
        this.maxSegmentSize = maxSegmentSize;
        this.maxRecordBytesPerSegment = maxSegmentSize - FileSystemWALService.WAL_HEADER_TOTAL_CAPACITY;
    }

    /**
     * Register a callback invoked whenever a brand new segment has been created on disk. The
     * callback runs while the segment lock is held so the caller can safely seed the segment file
     * (for example, by writing an initial header) before any subsequent reader can observe it.
     */
    void setInitializer(SegmentInitializer initializer) {
        this.initializer = initializer;
    }

    /**
     * Initialization hook fired once when a new segment file is created.
     */
    interface SegmentInitializer {
        void initialize(Segment segment) throws IOException;
    }

    File directory() {
        return directory;
    }

    long maxSegmentSize() {
        return maxSegmentSize;
    }

    long maxRecordBytesPerSegment() {
        return maxRecordBytesPerSegment;
    }

    /**
     * Scan the directory and open every existing segment file. Should be called once during startup.
     */
    void load() throws IOException {
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
        lock.lock();
        try {
            for (File file : files) {
                long index = Segment.parseSegmentIndex(file.getName());
                if (index < 0) {
                    continue;
                }
                Segment segment = new Segment(file, index, maxSegmentSize);
                segment.openOrCreate();
                Segment prev = segments.put(segment.startOffset(), segment);
                if (prev != null) {
                    prev.close();
                }
            }
        } finally {
            lock.unlock();
        }
        LOGGER.info("loaded {} WAL segment(s) from {}", segments.size(), directory);
    }

    /**
     * Returns whether the directory has any segments yet.
     */
    boolean isEmpty() {
        lock.lock();
        try {
            return segments.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Snapshot of all segments ordered by start offset (read-only view).
     */
    List<Segment> snapshot() {
        lock.lock();
        try {
            return new ArrayList<>(segments.values());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find the segment whose offset range covers {@code globalOffset}. Returns {@code null} if no
     * such segment is currently loaded.
     */
    Segment segmentForOffset(long globalOffset) {
        lock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.floorEntry(globalOffset);
            if (entry == null) {
                return null;
            }
            Segment segment = entry.getValue();
            return segment.containsOffset(globalOffset) ? segment : null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find the smallest segment whose start offset is greater than {@code globalOffset}. Useful
     * during recovery when a hole in the segment range needs to be skipped (e.g. earlier segments
     * have been trimmed and deleted).
     */
    Segment nextSegmentAfter(long globalOffset) {
        lock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.higherEntry(globalOffset);
            return entry == null ? null : entry.getValue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the segment that owns {@code globalOffset}, creating it on disk (and any missing
     * segments in between) if necessary.
     */
    Segment getOrCreateSegmentForOffset(long globalOffset) throws IOException {
        long index = Segment.indexForOffset(globalOffset, maxRecordBytesPerSegment);
        System.err.println("[DEBUG] getOrCreate: before lock, index=" + index + " offset=" + globalOffset);
        lock.lock();
        try {
            System.err.println("[DEBUG] getOrCreate: acquired lock, index=" + index);
            long startOffset = index * maxRecordBytesPerSegment;
            Segment segment = segments.get(startOffset);
            if (segment != null) {
                return segment;
            }
            File file = new File(directory, Segment.buildFileName(index));
            boolean newFile = !file.exists();
            segment = new Segment(file, index, maxSegmentSize);
            System.err.println("[DEBUG] getOrCreate: before openOrCreate, index=" + index);
            segment.openOrCreate();
            System.err.println("[DEBUG] getOrCreate: after openOrCreate, before init, index=" + index + " newFile=" + newFile);
            if (newFile && initializer != null) {
                initializer.initialize(segment);
            }
            System.err.println("[DEBUG] getOrCreate: after init, index=" + index);
            segments.put(segment.startOffset(), segment);
            LOGGER.info("created new WAL segment {}", segment);
            return segment;
        } finally {
            lock.unlock();
            System.err.println("[DEBUG] getOrCreate: released lock, index=" + index);
        }
    }

    /**
     * Returns the most recently created segment by start offset.
     */
    Segment latestSegment() {
        lock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.lastEntry();
            return entry == null ? null : entry.getValue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the highest global offset that is reachable from the currently loaded segments
     * (i.e. the exclusive end offset of the latest segment, or 0 if no segment exists yet).
     */
    long highestKnownEndOffset() {
        lock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.lastEntry();
            return entry == null ? 0 : entry.getValue().endOffsetExclusive();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Delete every segment whose end offset is less than or equal to {@code trimOffset + 1}. In
     * other words, segments that contain no offset greater than {@code trimOffset}.
     */
    int deleteSegmentsBelowOrEqual(long trimOffset) {
        if (trimOffset < 0) {
            return 0;
        }
        int deleted = 0;
        lock.lock();
        try {
            Iterator<Map.Entry<Long, Segment>> iter = segments.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Segment> entry = iter.next();
                Segment segment = entry.getValue();
                // The latest segment is never deleted to keep trim metadata persisted.
                if (segments.size() == 1) {
                    break;
                }
                if (segment.endOffsetExclusive() <= trimOffset + 1) {
                    iter.remove();
                    segment.deleteQuietly();
                    LOGGER.info("trimmed segment {}", segment);
                    deleted++;
                } else {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
        return deleted;
    }

    /**
     * Close every open segment.
     */
    void close() {
        lock.lock();
        try {
            for (Segment segment : segments.values()) {
                segment.close();
            }
            segments.clear();
        } finally {
            lock.unlock();
        }
    }
}
