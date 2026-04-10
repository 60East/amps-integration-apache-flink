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

import java.beans.ExceptionListener;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.DefaultBookmarkStore;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.ReconnectDelayStrategy;
import com.crankuptheamps.client.ServerChooser;
import com.crankuptheamps.client.exception.AMPSException;
import com.crankuptheamps.client.fields.Field;
import com.crankuptheamps.flink.source.reader.deserializer.AMPSDeserializationSchema;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.source.split.AMPSSplitState;
import com.crankuptheamps.flink.util.AMPSMessage;
import com.crankuptheamps.flink.util.AMPSQueueSemantics;
import com.crankuptheamps.flink.util.AMPSSourceHeaderKeys;
import com.crankuptheamps.flink.util.function.BookmarkStoreFunction;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link org.apache.flink.api.connector.source.SourceReader} for the {@link com.crankuptheamps.flink.source.AMPSSource}. 
 *
 * It connects to AMPS, subscribes based on its given splits, and emits the received messages as records.
 *
 * @param <OUT> The output type of the source.
 */
@Internal
public class AMPSSourceReader<OUT> extends SingleThreadMultiplexSourceReaderBase<AMPSMessage, OUT, AMPSSplit, AMPSSplitState>  {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMPSSourceReader.class);
    
    /** The shared client. */
    protected final HAClient client;
    /** The queue semantics for this reader. */
    protected final AMPSQueueSemantics queueSemantics;
    /** Flag for if bookmarks are discarded after a record is emitted. */
    protected final boolean discardAfterEmit;

    /**
     * Creates a new {@link AMPSSourceReader}.
     *
     * @param context                   The context for the reader.
     * @param deserializationSchema     The schema to deserialize messages from AMPS.
     * @param uri                       The connection URI for the AMPS server.
     * @param topic                     The AMPS topic to subscribe to.
     * @param clientName                The name to identify this client in AMPS.
     * @param contentFilter             The content filter for the subscription.
     * @param options                   Additional options for the subscription command.
     * @param bookmarkStoreFunction     The bookmark store function for getting bookmark stores.
     * @param serverChooser             The server chooser for advanced HA.
     * @param bookmark                  The starting bookmark for the subscription.
     * @param queueSemantics            AMPS queue semantics.
     * @param ackBatchSize              The ack batch size of messages.
     * @param ackTimeout                The ack timeout for messages.
     * @param topN                      The maximum number of messages to provide.
     * @param deliveryGuarantee         The DeliveryGuarantee for the SourceReader.
     * @param subscribeCommand          The subscription command the SourceReader will use.
     * @param exceptionListener         The exception listener that the SourceReader will use.
     * @param internalBufferSize        The size of the queue used to buffer messages.
     * @param batchSize                 The batch size for this client's commands.
     * @param orderBy                   The String for how SOW results will be ordered.
     * @param reconnectDelayStrategy    The ReconnectDelayStrategy to use.
     * @param heartbeat                 The heartbeat interval the reader will use.
     * @param client                    The HAClient the reader will use.
     * @param config                    The Configuration that will be used to supply options to the SourceBaseReader.
     * @param skipN                     The number of messages to skip.
     * @param maxBacklog                The max backlog of messages from a queue.
     * @param useSuffix                 Flag for adding a number to the client name.
     * @param pruneInterval             Interval in milliseconds for when to prune a LoggedBookmarkStore.
     * @param sleepMillisAfterBlock     The amount of milliseconds to sleep for after blocking.
     * @param discardAfterEmit          Flag for when bookmarks are discarded.
     * @param headerKeys                The headers to preserve from a message.
     * @param initCallback              The functional interface called before the client connects to AMPS.
     */
    public AMPSSourceReader(SourceReaderContext context, AMPSDeserializationSchema<OUT> deserializationSchema,
                           String uri, String topic, String clientName, String contentFilter, String options,
                           BookmarkStoreFunction bookmarkStoreFunction, ServerChooser serverChooser, String bookmark, AMPSQueueSemantics queueSemantics,
                           int ackBatchSize, int ackTimeout, int topN, DeliveryGuarantee deliveryGuarantee, String subscribeCommand, 
                           ExceptionListener exceptionListener, int internalBufferSize, int batchSize, String orderBy, 
                           ReconnectDelayStrategy reconnectDelayStrategy, int heartbeat, HAClient client, Configuration config,
                           int skipN, int maxBacklog, boolean useSuffix, long pruneInterval, int sleepMillisAfterBlock, 
                           boolean discardAfterEmit, Set<AMPSSourceHeaderKeys> headerKeys, ConnectorInitializer initCallback) {
        super(
            () -> new AMPSSplitReader(context, uri, topic, clientName, contentFilter, options, 
                                        bookmarkStoreFunction, serverChooser, bookmark, queueSemantics, ackBatchSize, ackTimeout, topN, 
                                        deliveryGuarantee, subscribeCommand, exceptionListener, internalBufferSize, batchSize, 
                                        orderBy, reconnectDelayStrategy, heartbeat, client, skipN, maxBacklog, useSuffix, 
                                        sleepMillisAfterBlock, headerKeys, initCallback),
            new AMPSRecordEmitter<>(context, options, queueSemantics, client, discardAfterEmit, pruneInterval, deserializationSchema),
            config,
            context);

        this.client = client;
        this.queueSemantics = queueSemantics;
        this.discardAfterEmit = discardAfterEmit;
    }

    /**
     * Requests more splits if this reader has no more splits.
     *
     * @param finishedSplitIds The finished splits.
     */
    @Override
    public void onSplitFinished(Map<String, AMPSSplitState> finishedSplitIds) {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            LOGGER.info("Requesting more splits");
            context.sendSplitRequest();
        }
    }

    /**
     * Creates the initial {@link AMPSSplitState} by using an {@link AMPSSplit}.
     *
     * @param split The split being converted to a mutable split.
     * @return A mutable split that can be used to keep track of a subscription.
     */
    @Override
    public AMPSSplitState initializedState(AMPSSplit split) {
        return split.toAMPSSplitState();
    }

    /**
     * Converts an {@link AMPSSplitState} to an {@link AMPSSplit} by using the client's bookmark store
     * to get the most recent checkpoint.
     *
     * @param splitId The split ID of the split being converted.
     * @param splitState The state of the split that is being converted.
     * @return An immutable split that can be used to resume a subscription.
     */
    @Override
    public AMPSSplit toSplitType(String splitId, AMPSSplitState splitState) {
        String newSplitBookmark = null;

        if (splitState.getSubId() != null && !(client.getBookmarkStore() instanceof DefaultBookmarkStore)) {
            try {
                newSplitBookmark = new String(client.getBookmarkStore().getMostRecent(splitState.getSubId()).buffer, StandardCharsets.UTF_8);
            } catch (AMPSException ae) {
                LOGGER.error("Could not create AMPSSplit for id '{}' and subId '{}'", splitId, splitState.getSubId(), ae);
                throw new RuntimeException(ae);
            }
        }

        AMPSSplit split = splitState.toAMPSSplit(newSplitBookmark);

        if (discardAfterEmit) {
            LOGGER.debug("Discarded {} bookmark(s) when emitting records", splitState.getBookmarksDiscarded());
            splitState.resetBookmarksDiscarded();
        }

        LOGGER.debug("Created new split {}", split);

        return split;
    }

    /**
     * Snapshots the state of the source reader.
     *
     * @param checkpointId The ID of the checkpoint to snapshot the current state to.
     * @return A list of splits that represent the current state of the reader.
     */
    @Override
    public List<AMPSSplit> snapshotState(long checkpointId) {
        ((AMPSRecordEmitter<OUT>) recordEmitter).snapshotPendingMessages(checkpointId);

        if (queueSemantics == AMPSQueueSemantics.NONE) {
            try {
                discardBookmarks(checkpointId);
            } catch (Exception e) {
                LOGGER.error("Failed to discard bookmarks while snapshotting state", e);
                throw new RuntimeException(e);
            }
        }

        return super.snapshotState(checkpointId);
    }

    /**
     * Called when a checkpoint successfully completes.
     *
     * When a checkpoint completes, it must subsume all checkpoints before it.
     * This means acking the messages.
     *
     * @param checkpointId The ID of the checkpoint that completed.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (queueSemantics != AMPSQueueSemantics.AT_LEAST_ONCE) return;

        Map<Long, List<AMPSMessage>> messageMap = ((AMPSRecordEmitter<OUT>) recordEmitter).getPendingMessagesByCheckpoint();
        
        for (long checkpointKey : messageMap.keySet()) {
            // Subsume all checkpoints that came before the checkpointId
            if (checkpointKey <= checkpointId) {
                List<AMPSMessage> completedMessages = messageMap.remove(checkpointKey);
                
                if (completedMessages == null) continue;

                LOGGER.debug("Acking {} message(s)", completedMessages.size());

                for (AMPSMessage message : completedMessages) {
                    byte[] topic = message.getTopicRaw();
                    byte[] bookmark = message.getBookmarkRaw();

                    client.ack(topic, 0, topic.length,
                                bookmark, 0, bookmark.length);
                }
            }
        }
    }

    /**
     * Called if a checkpoint is aborted.
     * 
     * Because of the checkpointing subsuming contract, this will only affect the
     * map if it is an "at-least-once" queue to ensure all the messages that
     * were emitted are canceled rather than acked in {@link #notifyCheckpointComplete(long)}.
     *
     * @param checkpointId The ID of the checkpoint being aborted.
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (queueSemantics != AMPSQueueSemantics.AT_LEAST_ONCE) return;

        Map<Long, List<AMPSMessage>> messageMap = ((AMPSRecordEmitter<OUT>) recordEmitter).getPendingMessagesByCheckpoint();
        List<AMPSMessage> abortedMessages = messageMap.remove(checkpointId);

        if (abortedMessages != null) {
            LOGGER.debug("Canceling {} message(s)", abortedMessages.size());

            byte[] optionBytes = "cancel".getBytes(StandardCharsets.UTF_8);

            for (AMPSMessage message : abortedMessages) {
                if (message == null) continue;
                
                byte[] topic = message.getTopicRaw();
                byte[] bookmark = message.getBookmarkRaw();

                client.ack(topic, 0, topic.length,
                            bookmark, 0, bookmark.length,
                            optionBytes, 0, optionBytes.length);
            }
        }
    }

    /**
     * Called when Flink's internal state is being updated.
     *
     * Discards all messages that occurred before the checkpoint.
     *
     * @param checkpointId The ID of the checkpoint that contains the bookmarks that need to be discarded.
     */
    private void discardBookmarks(long checkpointId) throws Exception {
        BookmarkStore bookmarkStore = client.getBookmarkStore();

        Map<Long, List<AMPSMessage>> messageMap = ((AMPSRecordEmitter<OUT>) recordEmitter).getPendingMessagesByCheckpoint();

        for (long checkpointKey : messageMap.keySet()) {
            // Subsume all checkpoints that came before the checkpointId
            if (checkpointKey <= checkpointId) {
                List<AMPSMessage> completedMessages = messageMap.remove(checkpointKey);

                if (bookmarkStore == null || bookmarkStore instanceof DefaultBookmarkStore) {
                    LOGGER.warn("Checkpointing without a BookmarkStore for recovery");
                    continue;
                }

                if (completedMessages == null) continue;
        
                int bookmarksToDiscard = 0;
                for (AMPSMessage message : completedMessages) {
                    long discardSeqNo = bookmarkStore.getOldestBookmarkSeq(message.getSubIdRaw());
                    long mostRecentSeqNo = message.getBookmarkSeqNo();
                    if (discardSeqNo > -1 && discardSeqNo <= mostRecentSeqNo) {
                        bookmarksToDiscard += (mostRecentSeqNo - discardSeqNo) + 1;
                    }
                }
                LOGGER.debug("Discarded {} bookmark(s) on snapshot", bookmarksToDiscard);

                for (AMPSMessage message : completedMessages) {
                    long discardSeqNo;
                    long mostRecentSeqNo = message.getBookmarkSeqNo();
                    Field subId = message.getSubIdRaw();

                    do {
                        discardSeqNo = bookmarkStore.getOldestBookmarkSeq(subId);
                        if (discardSeqNo <= mostRecentSeqNo) {
                            bookmarkStore.discard(subId, discardSeqNo);
                        }
                    } while (discardSeqNo > -1 && discardSeqNo <= mostRecentSeqNo);
                }
            }
        }

        ((AMPSRecordEmitter<OUT>) recordEmitter).pruneBookmarkStore(bookmarkStore);
    }

    @VisibleForTesting
    public HAClient getClient() {
        return client;
    }

    @VisibleForTesting
    public AMPSRecordEmitter<OUT> getRecordEmitter() {
        return (AMPSRecordEmitter<OUT>) recordEmitter;
    }
}

