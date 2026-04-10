////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2024-2026 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////

package com.crankuptheamps.flink.source.reader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.crankuptheamps.client.AMPSTimestamp;
import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.flink.source.reader.deserializer.AMPSDeserializationSchema;
import com.crankuptheamps.flink.source.split.AMPSSplitState;
import com.crankuptheamps.flink.util.AMPSMessage;
import com.crankuptheamps.flink.util.AMPSQueueSemantics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link RecordEmitter} that emits records to Flink.
 *
 * Also responsible for keeping track of messages that need to be acked and bookmarks that need to be discarded.
 *
 * @param <OUT> The output type of the source.
 */
@Internal
public class AMPSRecordEmitter<OUT> implements RecordEmitter<AMPSMessage, OUT, AMPSSplitState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMPSRecordEmitter.class);

    private final SourceOutputWrapper<OUT> sourceOutputWrapper;

    /** The context for the source reader. */
    protected final SourceReaderContext context;
    /** Flag for if timestamps should be emitted. */
    protected final boolean includeTimestamps;
    /** The queue semantics being used for the reader. */
    protected final AMPSQueueSemantics queueSemantics;
    /** The client used to ack messages from an "at-most-once" queue. */
    protected final HAClient client;
    /** Map that holds the messages that have been emitted to Flink and have been snapshotted but have not been acknowledged yet. */
    protected final Map<Long, List<AMPSMessage>> pendingMessagesByCheckpoint = new ConcurrentHashMap<>();
    /** Map that holds the most recently emitted messages. */
    protected final Map<Integer, AMPSMessage> lastEmittedMessages = new ConcurrentHashMap<>();
    /** AMPSTimestamp object to help convert the byte array of an AMPS timestamp to a long. */
    protected final AMPSTimestamp ats = new AMPSTimestamp("19700101T000000.000000Z");
    /** Flag for if bookmarks should be discarded immediately after emitting records. */
    protected final boolean discardAfterEmit;
    /** Interval for pruning in milliseconds. */
    protected final long pruneInterval;
    /** The schema used to deserialize and process messages from AMPS. */
    protected final AMPSDeserializationSchema<OUT> deserializationSchema;

    /** List that holds the messages that have been emitted to Flink but have not been snapshotted. */
    protected List<AMPSMessage> pendingMessages = new ArrayList<>();
    /** Counter for determining when to prune a {@link LoggedBookmarkStore}. */
    protected long timeOfLastPrune = System.currentTimeMillis();
    
    /**
     * Constructor for an {@link AMPSRecordEmitter}.
     */
    public AMPSRecordEmitter(SourceReaderContext context, String options, AMPSQueueSemantics queueSemantics, HAClient client,
                                boolean discardAfterEmit, long pruneInterval, AMPSDeserializationSchema<OUT> deserializationSchema) {
        this.context = context;
        this.includeTimestamps = options == null ? 
            false : 
            options.contains(Message.Options.Timestamp) || options.contains("timestamp");
        this.queueSemantics = queueSemantics;
        this.client = client;
        this.discardAfterEmit = discardAfterEmit;
        this.pruneInterval = pruneInterval;
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<>(includeTimestamps);
        
        try {
            this.deserializationSchema.open(new DeserializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return context.metricGroup();
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return context.getUserCodeClassLoader();
                }
            });
        } catch (Exception e) {
            LOGGER.error("Failed to open deserialization schema {}", deserializationSchema, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Emits a record after deserialization and sets up for an ack or acks the message if necessary.
     */
    @Override
    public void emitRecord(AMPSMessage element, SourceOutput<OUT> output, AMPSSplitState splitState) throws Exception {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            if (includeTimestamps) {
                sourceOutputWrapper.setTimestamp(convertTimestamp(element.getTimestampRaw()));
            }
            deserializationSchema.deserialize(element, sourceOutputWrapper);
            
            splitState.setSubId(element.getSubIdRaw());

            if (discardAfterEmit) {
                BookmarkStore bookmarkStore = client.getBookmarkStore();

                bookmarkStore.discard(splitState.getSubId(), element.getBookmarkSeqNo());
                splitState.incBookmarksDiscarded();

                pruneBookmarkStore(bookmarkStore);
            } else {
                lastEmittedMessages.put(element.getSplitIndex(), element);
            }

            switch (queueSemantics) {
                case AT_MOST_ONCE -> {
                    byte[] topic = element.getTopicRaw();
                    byte[] bookmark = element.getBookmarkRaw();

                    client.ack(
                            topic, 0, topic.length,
                            bookmark, 0, bookmark.length);
                }
                case AT_LEAST_ONCE -> {
                    pendingMessages.add(element);
                }
                case NONE -> {}
            }
        } catch (Exception e) {
            LOGGER.error("Failed to emit record", e);
            throw new Exception(e);
        }
    }

    /**
     * Snapshots all pending messages by putting them in the map with the key being the checkpointId.
     */
    public void snapshotPendingMessages(long checkpointId) {
        if (queueSemantics == AMPSQueueSemantics.AT_LEAST_ONCE) {
            // For at-least-once, every message must be acked on checkpoint completion
            pendingMessagesByCheckpoint.put(checkpointId, pendingMessages);
            pendingMessages = new ArrayList<>();
        } else if (queueSemantics == AMPSQueueSemantics.NONE && !lastEmittedMessages.isEmpty()) {
            // For none, only the last emitted message is needed to discard all previous bookmarks
            pendingMessagesByCheckpoint.put(checkpointId, new ArrayList<>(lastEmittedMessages.values()));
        }
        // At-most-once queues already acked messages, so nothing needs to be snapshotted
    }

    /**
     * Gets all pending messages that have been snapshotted.
     */
    public Map<Long, List<AMPSMessage>> getPendingMessagesByCheckpoint() {
        return pendingMessagesByCheckpoint;
    }

    /**
     * Gets currently pending messages.
     */
    public List<AMPSMessage> getPendingMessages() {
        return pendingMessages;
    }

    /**
     * Gets the map of last emitted messages where the subscription ID is the key.
     */
    public Map<Integer, AMPSMessage> getLastEmittedMessages() {
        return lastEmittedMessages;
    }

    /**
     * Prunes the bookmark store if it is a {@link LoggedBookmarkStore}.
     */
    public void pruneBookmarkStore(BookmarkStore bookmarkStore) throws Exception {
        if (bookmarkStore instanceof LoggedBookmarkStore && System.currentTimeMillis() - timeOfLastPrune >= pruneInterval) {
            LOGGER.debug("Pruning LoggedBookmarkStore at {}", System.currentTimeMillis());
            ((LoggedBookmarkStore) bookmarkStore).prune();
            timeOfLastPrune = System.currentTimeMillis();
            LOGGER.debug("Finished pruning LoggedBookmarkStore at {}", timeOfLastPrune);
        }
    }

    /**
     * Converts the byte array of an AMPS timestamp in a field to a long.
     */
    @VisibleForTesting
    public long convertTimestamp(byte[] timestamp) {
        if (timestamp == null) return Long.MIN_VALUE;

        try {
            AMPSTimestamp.parse(timestamp, 0, timestamp.length, ats);

            return toEpochMillisUTC(ats.year, ats.month, ats.day, ats.hour, ats.minute, ats.second, ats.nano / 1_000_000);
        } catch (Exception e) {
            String ts = timestamp == null ? "null" : new String(timestamp, StandardCharsets.UTF_8);
            LOGGER.error("Failed to convert AMPS timestamp: {}", ts, e);
            throw new RuntimeException("Exception while translating timestamp '" + ts + "'", e);
        }
    }
        
    /**
     * Converts arguments into a long that represents milliseconds since the UNIX epoch.
     *
     * Does not perform any validation.
     */
    @VisibleForTesting
    public long toEpochMillisUTC(int year, int month, int day, int hour, int minute, int second, int millis) {
        long totalDays = 365 * year +
            (year + 3) / 4 -
            (year + 99) / 100 +
            (year + 399) / 400 +
            (367 * month - 362) / 12 +
            day - 1;

        if (month > 2) {
            if (((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0)) {
                // Is leap year, so subtract 1
                totalDays -= 1;
            } else {
                // Not leap year, so subtract 2
                totalDays -= 2;
            }
        }

        // Subtract days from year 0 to 1970 (UNIX epoch)
        totalDays -= 719528;

        long seconds = totalDays * 86_400 +
            hour * 3_600 +
            minute * 60 +
            second;

        return seconds * 1_000L + millis;
    }

    private static class SourceOutputWrapper<OUT> implements Collector<OUT> {
        
        private final boolean includeTimestamps;

        private SourceOutput<OUT> sourceOutput;
        private long timestamp;

        public SourceOutputWrapper(boolean includeTimestamps) {
            this.includeTimestamps = includeTimestamps;
        }

        @Override
        public void collect(OUT record) {
            if (includeTimestamps) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Emit record {} with timestamp {}", record, timestamp);
                }

                sourceOutput.collect(record, timestamp);
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Emit record {}", record);
                }

                sourceOutput.collect(record);
            }
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<OUT> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}

