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
import java.util.Iterator;
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

    /**
     * All known segments, keyed by start offset. Sorted in ascending order. Access must be
     * synchronized via {@link #segmentsLock}.
     */
    private final NavigableMap<Long, Segment> segments = new TreeMap<>();
    private final ReentrantLock segmentsLock = new ReentrantLock();

    public SegmentManager(File directory) {
        this.directory = directory;
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
                Segment segment = new Segment(file, startOffset);
                segment.openOrCreate();
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
     * Find the smallest segment whose start offset is greater than {@code walLogicalOffset}. Useful
     * during recovery when a hole in the segment range needs to be skipped (e.g. earlier segments
     * have been trimmed and deleted).
     */
    public Segment nextSegmentAfter(long walLogicalOffset) {
        segmentsLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.higherEntry(walLogicalOffset);
            return entry == null ? null : entry.getValue();
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Get the segment that should own {@code walLogicalOffset}. If the offset falls within an existing
     * segment, that segment is returned. Otherwise a new segment is created with
     * {@code startOffset = walLogicalOffset}.
     * <p>
     * For append-only writes, the caller should ensure offsets are monotonically increasing so that
     * the latest segment naturally covers the new offset or a new segment is created.
     */
    public Segment getOrCreateSegmentForOffset(long walLogicalOffset) throws IOException {
        segmentsLock.lock();
        try {
            // Check if any existing segment covers this offset.
            Map.Entry<Long, Segment> entry = segments.floorEntry(walLogicalOffset);
            if (entry != null) {
                Segment segment = entry.getValue();
                // The segment covers this offset if it's within the written range, or if it's
                // the latest segment and the offset is at or beyond its current end (append case).
                if (segment.containsOffset(walLogicalOffset) || walLogicalOffset == segment.endOffsetExclusive()) {
                    return segment;
                }
                // Check if this is the latest segment - appends always go to the latest segment.
                Map.Entry<Long, Segment> lastEntry = segments.lastEntry();
                if (lastEntry != null && lastEntry.getValue() == segment) {
                    // This is the latest segment but the offset is beyond its end - still use it
                    // for append (the offset should be contiguous).
                    return segment;
                }
            }
            // No existing segment covers this offset; create a new one.
            return createNewSegment(walLogicalOffset);
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Create a new segment starting at the given offset. Must be called while holding {@link #segmentsLock}.
     */
    private Segment createNewSegment(long startOffset) throws IOException {
        File file = new File(directory, Segment.buildFileName(startOffset));
        Segment segment = new Segment(file, startOffset);
        segment.openOrCreate();
        segments.put(segment.startOffset(), segment);
        LOGGER.info("created new WAL segment {}", segment);
        return segment;
    }

    /**
     * Create a new segment explicitly with the given start offset. This is used when the caller
     * knows a new segment should be started (e.g. rollover).
     */
    public Segment createSegment(long startOffset) throws IOException {
        segmentsLock.lock();
        try {
            return createNewSegment(startOffset);
        } finally {
            segmentsLock.unlock();
        }
    }

    /**
     * Returns the most recently created segment by start offset.
     */
    public Segment latestSegment() {
        segmentsLock.lock();
        try {
            Map.Entry<Long, Segment> entry = segments.lastEntry();
            return entry == null ? null : entry.getValue();
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
        if (trimOffset < 0) {
            return 0;
        }
        int deleted = 0;
        segmentsLock.lock();
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
        } finally {
            segmentsLock.unlock();
        }
    }
}
