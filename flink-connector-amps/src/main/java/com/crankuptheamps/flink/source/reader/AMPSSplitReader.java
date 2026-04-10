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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.DefaultBookmarkStore;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.MemoryBookmarkStore;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.ReconnectDelayStrategy;
import com.crankuptheamps.client.ServerChooser;
import com.crankuptheamps.client.exception.AMPSException;
import com.crankuptheamps.client.exception.DisconnectedException;
import com.crankuptheamps.client.fields.Field;
import com.crankuptheamps.client.fields.StringField;
import com.crankuptheamps.flink.source.checkpointing.AMPSFlinkRP;
import com.crankuptheamps.flink.source.checkpointing.AMPSFlinkRPA;
import com.crankuptheamps.flink.source.checkpointing.AMPSFlinkRPF;
import com.crankuptheamps.flink.source.metrics.SourceMetrics;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.util.AMPSMessage;
import com.crankuptheamps.flink.util.AMPSQueueSemantics;
import com.crankuptheamps.flink.util.AMPSSourceHeaderKeys;
import com.crankuptheamps.flink.util.ClientNamePool;
import com.crankuptheamps.flink.util.function.BookmarkStoreFunction;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SplitReader} used to read from AMPS based on splits.
 */
@Internal
public class AMPSSplitReader implements SplitReader<AMPSMessage, AMPSSplit> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AMPSSplitReader.class);
    /** Map of client names to use. */
    private static final Map<String, ClientNamePool> clientNames = new HashMap<>();
 
    /** The context for the source reader. */
    protected final SourceReaderContext context;
    /** The connection URI for the AMPS server. */
    protected final String uri;
    /** The AMPS topic to subscribe to. */
    protected final String topic;
    /** The original client name without the suffix. */
    protected final String originalName;
    /** The name to identify this client in AMPS. */
    protected final String clientName;
    /** The content filter for the command. */
    protected final String contentFilter;
    /** Additional options for the subscription command. */
    protected final String options;
    /** The bookmark store for durable subscriptions. */
    protected final BookmarkStore bookmarkStore;
    /** The server chooser for advanced HA. */
    protected final ServerChooser serverChooser;
    /** The starting bookmark for the subscription. */
    protected final String bookmark;
    /** Holds the semantics of the message queue from AMPS. */
    protected final AMPSQueueSemantics queueSemantics;
    /** The ack batch size of messages. */
    protected final int ackBatchSize;
    /** The ack timeout for messages. */
    protected final int ackTimeout;
    /** The AMPS High-Availability client instance. */
    protected final HAClient client;
    /** The maximum number of messages from a command that the client should read. */
    protected final int topN;
    /** The delivery guarantee for this reader. */
    protected final DeliveryGuarantee deliveryGuarantee;
    /** The command to use. */
    protected final String subscribeCommand;
    /** The exception listener that will communicate the absorbed exceptions. */
    protected final ExceptionListener exceptionListener;
    /** The buffer size used for messages. */
    protected final int internalBufferSize;
    /** The batch size the SourceReader will use. */
    protected final int batchSize;
    /** The string for how SOW results will be ordered. */
    protected final String orderBy;
    /** The reconnect delay strategy client will use. */
    protected final ReconnectDelayStrategy reconnectDelayStrategy;
    /** The interval seconds for the heartbeat. */
    protected final int heartbeat;
    /** The amount of messages to skip. */
    protected final int skipN;
    /** The max backlog if subscribed to a queue. */
    protected final int maxBacklog;
    /** A queue to store the received messages. */
    protected final BlockingQueue<AMPSMessage> messages;
    /** The list that contains information about this reader's commands. */
    protected final List<ReaderCommand> commands = new ArrayList<>();
    /** Used to help recover a bookmark sub when the given bookmark store is null. */
    protected final AMPSFlinkRPA recoveryPointAdapter = new AMPSFlinkRPA();
    /** Flag for if the timestamp option from {@link Message.Options} is included in the options. */
    protected final boolean includeTimestamps;
    /** Counter for connects. */
    protected final Counter connects;
    /** Counter for disconnects. */
    protected final Counter disconnects;
    /** Counter for bytes received from AMPS. */
    protected final Counter bytesFromAMPS;
    /** A list of splits that are being removed and need to be added to a {@link RecordsWithSplitIds}. */
    protected final List<AMPSSplit> splitsToRemove = new ArrayList<>();
    /** The amount of milliseconds to sleep for after blocking. */
    protected final int sleepMillisAfterBlock;
    /** The set of header keys to preserve from a message. */
    protected final Set<AMPSSourceHeaderKeys> headerKeys;
    /** The functional interface that will be called before an {@link HAClient} connects to AMPS. */
    protected final ConnectorInitializer initCallback;
    /** The message used to wake up the reader. */
    protected final AMPSMessage poison = new AMPSMessage("Poison Message. Should not appear.".getBytes());
    
    /** Flag for if the buffer has a poisoned message. */
    protected volatile boolean poisoned = false;

    /** The charset used by the reader's client. */
    protected Charset charset = StandardCharsets.UTF_8;

    /**
     * Constructor for the reader.
     */
    public AMPSSplitReader(SourceReaderContext context, String uri, String topic, String clientName, String contentFilter, 
                            String options, BookmarkStoreFunction bookmarkStoreFunction, ServerChooser serverChooser, String bookmark, 
                            AMPSQueueSemantics queueSemantics, int ackBatchSize, int ackTimeout, int topN, 
                            DeliveryGuarantee deliveryGuarantee, String subscribeCommand, ExceptionListener exceptionListener,
                            int internalBufferSize, int batchSize, String orderBy, ReconnectDelayStrategy reconnectDelayStrategy, 
                            int heartbeat, HAClient client, int skipN, int maxBacklog, boolean useSuffix, int sleepMillisAfterBlock,
                            Set<AMPSSourceHeaderKeys> headerKeys, ConnectorInitializer initCallback) {
        this.context = context;
        this.uri = uri;
        this.topic = topic;
        this.originalName = clientName;

        if (useSuffix) {
            synchronized (clientNames) {
                ClientNamePool clientNamePool = clientNames.getOrDefault(originalName, new ClientNamePool());
                this.clientName = clientNamePool.getClientName(originalName);
                clientNames.put(originalName, clientNamePool);
            }
        } else {
            this.clientName = clientName;
        }
        
        this.contentFilter = contentFilter;
        this.options = options;

        if (bookmarkStoreFunction != null) {
            synchronized (bookmarkStoreFunction) {
                this.bookmarkStore = bookmarkStoreFunction.apply(this.clientName);
            }
        } else {
            this.bookmarkStore = null;
        }

        this.serverChooser = serverChooser;
        this.bookmark = bookmark;
        this.queueSemantics = queueSemantics;
        this.ackBatchSize = ackBatchSize;
        this.ackTimeout = ackTimeout;
        this.topN = topN;
        this.deliveryGuarantee = deliveryGuarantee;
        this.subscribeCommand = subscribeCommand;
        this.exceptionListener = exceptionListener;
        this.internalBufferSize = internalBufferSize;
        this.batchSize = batchSize;
        this.orderBy = orderBy;
        this.reconnectDelayStrategy = reconnectDelayStrategy;
        this.heartbeat = heartbeat;
        this.client = client;
        this.skipN = skipN;
        this.maxBacklog = maxBacklog;
        this.sleepMillisAfterBlock = sleepMillisAfterBlock;
        this.headerKeys = headerKeys;
        this.initCallback = initCallback;
        
        this.messages = new ArrayBlockingQueue<>(internalBufferSize);
        this.includeTimestamps = options == null ? 
            false : 
            options.contains(Message.Options.Timestamp) || options.contains("timestamp");

        this.connects = context.metricGroup()
            .addGroup(SourceMetrics.Group.SOURCE)
            .addGroup(SourceMetrics.Group.READER)
            .counter(SourceMetrics.Metric.CONNECTS);

        this.disconnects = context.metricGroup()
            .addGroup(SourceMetrics.Group.SOURCE)
            .addGroup(SourceMetrics.Group.READER)
            .counter(SourceMetrics.Metric.DISCONNECTS);
        
        context.metricGroup()
            .addGroup(SourceMetrics.Group.SOURCE)
            .addGroup(SourceMetrics.Group.READER)
            .gauge(SourceMetrics.Metric.USED_QUEUE_CAPACITY, () -> messages.size());

        context.metricGroup()
            .addGroup(SourceMetrics.Group.SOURCE)
            .addGroup(SourceMetrics.Group.READER)
            .gauge(SourceMetrics.Metric.REMAINING_QUEUE_CAPACITY, () -> messages.remainingCapacity());
        
        this.bytesFromAMPS = context.metricGroup().getIOMetricGroup().getNumBytesInCounter();

        try {
            startClient();
        } catch (Exception e) {
            LOGGER.error("Failed to start client", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetches records from the queue.
     */
    @Override
    public RecordsWithSplitIds<AMPSMessage> fetch() {
        RecordsBySplits.Builder<AMPSMessage> records = new RecordsBySplits.Builder<>();
        AMPSMessage head = null;
        int recordsFetched = 0;

        // Block until a message is taken from the buffer
        try {
            head = messages.take();

            if (sleepMillisAfterBlock > 0) {
                Thread.sleep(sleepMillisAfterBlock);
            }
        } catch (InterruptedException ie) {
            LOGGER.warn("Interrupted while fetching from buffer in '{}'", clientName);
        }

        // Add the removed splits if they exist
        if (!splitsToRemove.isEmpty()) {
            for (AMPSSplit split : splitsToRemove) {
                records.addFinishedSplit(split.splitId());
            }
            splitsToRemove.clear();
        }

        if (commands.size() == 1) {
            // If the reader only has one split, use drainTo
            List<AMPSMessage> drainedMessages = new ArrayList<>(messages.size() + 10);

            if (!poisoned || !head.equals(poison)) {
                // Add the head
                drainedMessages.add(head);
            } else {
                // Head is the poison, so set poisoned to false
                poisoned = false;
            }

            // Add other messages
            messages.drainTo(drainedMessages);

            if (poisoned) {
                // If poison still exists, attempt to remove it
                poisoned = !drainedMessages.remove(poison);
            }
            
            int lastMessageIndex = drainedMessages.size() - 1;
            if (lastMessageIndex >= 0 && drainedMessages.get(lastMessageIndex).getAckType() == Message.AckType.Completed) {
                // Update finished splits and remove the ack message
                records.addFinishedSplit(commands.get(0).split.splitId());
                drainedMessages.remove(lastMessageIndex);
            }
            
            recordsFetched = drainedMessages.size();
            records.addAll(commands.get(0).split.splitId(), drainedMessages);
        } else {
            if (!poisoned || !head.equals(poison)) {
                if (head.getAckType() == Message.AckType.Completed) {
                    // Head is a completed ack and should mark the split as completed
                    records.addFinishedSplit(commands.get(head.getSplitIndex()).split.splitId());
                } else {
                    // Head is a record and should be added
                    records.add(commands.get(head.getSplitIndex()).split.splitId(), head);
                }
            } else {
                // Head is the poison, so set poisoned to false
                poisoned = false;
            }

            // If the reader has multiple splits, poll through the queue
            int recordCount = 1;
            for (; messages.peek() != null && recordCount < internalBufferSize; recordCount++) {
                AMPSMessage message = messages.poll();

                if (poisoned && message.equals(poison)) {
                    // Poison was removed
                    poisoned = false;
                    recordCount--; // Since a record was not added, decrement the counter
                } else if (message.getAckType() == Message.AckType.Completed) {
                    // Do not add the ack message, only update finished splits
                    records.addFinishedSplit(commands.get(message.getSplitIndex()).split.splitId());
                    recordCount--; // Since a record was not added, decrement the counter
                } else {
                    // Add the message to records
                    records.add(commands.get(message.getSplitIndex()).split.splitId(), message);
                }
            }

            recordsFetched = recordCount;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Fetch {} record(s) from '{}'", recordsFetched, clientName);
        }
        
        return records.build();
    }

    /**
     * Determines how to handle split changes for the client.
     *
     * @param splitsChanges The splits that are changing.
     */
    @Override
    public void handleSplitsChanges(SplitsChange<AMPSSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            List<AMPSSplit> validSplits = new ArrayList<>();

            // Invalid splits need to be added to a RecordsWithSplitIds as a finished split
            for (AMPSSplit split : splitsChanges.splits()) {
                // For now, assume all splits are valid
                if (true) {
                    validSplits.add(split);
                } else {
                    splitsToRemove.add(split);
                }
            }

            addSplits(validSplits);
        } else if (splitsChanges instanceof SplitsRemoval) {
            // Removed splits need to be added to a RecordsWithSplitIds as a finished split
            splitsToRemove.addAll(splitsChanges.splits());

            removeSplits(splitsChanges.splits());
        } else {
            throw new UnsupportedOperationException("Unknown SplitsChange implementation");
        }
    }

    /**
     * Pauses/resumes splits.
     *
     * Called during watermark alignment.
     *
     * @param splitsToPause The splits that this reader should pause.
     * @param splitsToResume The splits that this reader should resume.
     */
    @Override
    public void pauseOrResumeSplits(Collection<AMPSSplit> splitsToPause, Collection<AMPSSplit> splitsToResume) {
        LOGGER.debug("Pausing {} and resuming {} split(s) for '{}'", splitsToPause.size(), splitsToResume.size(), clientName);
        removeSplits(splitsToPause);

        for (AMPSSplit splitToResume : splitsToResume) {
            LOGGER.debug("Resuming splits for '{}'", clientName);
            for (int i = 0; i < commands.size(); i++) {
                ReaderCommand possibleMatch = commands.get(i);

                // If the commandId is null, the subscription is paused, so it should be resumed in this case
                if (splitToResume.splitId().equals(possibleMatch.split.splitId()) && possibleMatch.commandId == null) {
                    try {
                        AMPSSplit resumeSub;
                        
                        if (client.getBookmarkStore() instanceof DefaultBookmarkStore) {
                            resumeSub = new AMPSSplit(
                                possibleMatch.split.getTopic(),
                                possibleMatch.split.getSplitFilter());
                        } else {
                            resumeSub = new AMPSSplit(
                                possibleMatch.split.getTopic(),
                                possibleMatch.split.getSplitFilter(),
                                client.getBookmarkStore().getMostRecent(possibleMatch.subId).toString());
                        }

                        possibleMatch.split = resumeSub;
                        executeCommand(possibleMatch, i);
                        break;
                    } catch (AMPSException ae) {
                        LOGGER.error("Failed to resume subscription for '{}'", clientName, ae);
                        throw new RuntimeException(ae);
                    }
                }
            }
        }
    }

    /**
     * Add poison if necessary to wake up {@link #fetch()}.
     */
    @Override
    public void wakeUp() {
        if (messages.isEmpty()) {
            poisoned = messages.offer(poison);
        }
    }

    /**
     * Closes the client.
     */
    @Override
    public void close() throws Exception {
        LOGGER.info("Closing client '{}'", clientName);

        try {
            client.unsubscribe();
        } catch (DisconnectedException de) {
            // Client is already disconnected and unsubscribed server-side
        } catch (Exception e) {
            LOGGER.error("Exception while unsubscribing client '{}'", clientName, e);
        } finally {
            client.close();
        }

        try {
            BookmarkStore currentBookmarkStore = client.getBookmarkStore();

            if (currentBookmarkStore instanceof LoggedBookmarkStore) {
                ((LoggedBookmarkStore) currentBookmarkStore).prune();
            }

            if (currentBookmarkStore != null) {
                currentBookmarkStore.close();
            }
        } catch (Exception e) {
            LOGGER.error("Exception while closing bookmark store for client '{}'", clientName, e);
        }
    
        synchronized (clientNames) {
            ClientNamePool clientNamePool = clientNames.get(originalName);
            if (clientNamePool != null) {
                clientNamePool.returnClientName(clientName);
                clientNames.put(originalName, clientNamePool);
            }
        }

        LOGGER.info("Closed client '{}'", clientName);
    }

    /**
     * Starts the client.
     *
     * @throws Exception If the client has an issue while starting.
     */
    private void startClient() throws Exception {
        LOGGER.info("Starting client '{}'", clientName);

        if (serverChooser != null) {
            LOGGER.debug("Set server chooser");
            client.setServerChooser(serverChooser);
        } else {
            LOGGER.debug("Create and set server chooser using URI");
            DefaultServerChooser defaultChooser = new DefaultServerChooser();
            defaultChooser.add(uri);
            client.setServerChooser(defaultChooser);
        }

        if (exceptionListener != null) {
            LOGGER.debug("Set exception listener");
            client.setExceptionListener(exceptionListener);
        }

        if (bookmarkStore != null) {
            LOGGER.debug("Set bookmark store");
            client.setBookmarkStore(bookmarkStore);
        }

        if (queueSemantics != AMPSQueueSemantics.NONE) {
            LOGGER.debug("Set ack batch size: {}", ackBatchSize);
            client.setAckBatchSize(ackBatchSize);
            LOGGER.debug("Set ack timeout: {}", ackTimeout);
            client.setTimeout(ackTimeout);
        }

        if (reconnectDelayStrategy != null) {
            LOGGER.debug("Set reconnect delay strategy");
            client.setReconnectDelayStrategy(reconnectDelayStrategy);
        }

        client.addConnectionStateListener((int newState) -> {
            switch (newState) {
                case ConnectionStateListener.Connected -> {
                    LOGGER.debug("Changed connection state: '{}' connected to AMPS", clientName);
                    connects.inc();
                }
                case ConnectionStateListener.Disconnected -> {
                    LOGGER.debug("Changed connection state: '{}' was disconnected from AMPS", clientName);
                    disconnects.inc();
                }
                default -> {}
            }
        });

        if (heartbeat > 0) {
            LOGGER.debug("Set heartbeat with interval: {}", heartbeat);
            client.setHeartbeat(heartbeat);
        }

        LOGGER.debug("Set name to '{}'", clientName);
        client.setName(clientName);

        if (sleepMillisAfterBlock > 0) {
            LOGGER.debug("Set sleep after blocking: {} millisecond(s)", sleepMillisAfterBlock);
        }

        if (initCallback != null) {
            LOGGER.debug("Entering provided connector initializer {}", initCallback);
            initCallback.init(client);
            LOGGER.debug("Exited provided connector initializer {}", initCallback);
        }

        if (client.getTransport() == null) {
            LOGGER.debug("Connect and logon client '{}'", clientName);
            client.connectAndLogon();
        }

        this.charset = client.allocateMessage().getDecoder().charset();

        LOGGER.info("Started client '{}'", client.getName());
    }
    
    /**
     * Removes subscriptions based on a collection of splits.
     *
     * @param splitsToPause The splits to pause/remove.
     */
    protected void removeSplits(Collection<AMPSSplit> splitsToPause) {
        for (AMPSSplit splitToPause : splitsToPause) {
            LOGGER.debug("Removing splits for '{}'", clientName);
            for (int i = 0; i < commands.size(); i++) {
                ReaderCommand possibleMatch = commands.get(i);

                if (splitToPause.splitId().equals(possibleMatch.split.splitId()) && possibleMatch.commandId != null) {
                    try {
                        client.unsubscribe(possibleMatch.commandId);
                        possibleMatch.commandId = null;
                    } catch (AMPSException ae) {
                        LOGGER.error("Failed to unsubscribe for '{}'", clientName, ae);
                        throw new RuntimeException(ae);
                    }
                    break; // Move to the next split to pause
                }
            }
        }
    }

    /**
     * Executes a command based on fields and an {@link AMPSSplit}.
     *
     * @param readerCommand The reader subscription the {@link Command} will be tied to.
     * @param subIndex The index of the reader subscription in the subs field.
     */
    protected void executeCommand(ReaderCommand readerCommand, int subIndex) {
        LOGGER.info("Adding '{}' command to '{}'", subscribeCommand, clientName);
        Command command = new Command(subscribeCommand);
        
        // Set the topic by prioritizing the topic from the split
        if (readerCommand.split.getTopic() != null && !readerCommand.split.getTopic().isEmpty()) {
            command.setTopic(readerCommand.split.getTopic());
        } else {
            command.setTopic(topic);
        }
        LOGGER.debug("Set topic '{}' for '{}' command", command.getTopic(), clientName);

        // Set filter based on the content filter from the source and the content filter from the split
        String splitContentFilter = readerCommand.split.getSplitFilter();
        if (contentFilter != null) {
            if (!splitContentFilter.isEmpty()) {
                command.setFilter("(" + contentFilter + ") AND (" + splitContentFilter + ")");
            } else {
                command.setFilter(contentFilter);
            }
        } else if (!splitContentFilter.isEmpty()) {
            command.setFilter(splitContentFilter);
        }
        if (command.getFilter() != null) {
            LOGGER.debug("Set content filter '{}' for '{}' command", command.getFilter(), clientName);
        }

        // Set options
        String commandOptions = "";
        if (options != null) {
            // Check for any duplicate set options
            
            if (skipN > 0 && !options.contains("skip_n=")) {
                commandOptions += String.format("skip_n=%d,", skipN);
            } else if (skipN > 0) {
                LOGGER.warn("The skipN option is set in both the options String and the builder setSkipN method. Ignoring setSkipN");
            }

            if (maxBacklog > 1 && !options.contains("max_backlog=")) {
                commandOptions += String.format("max_backlog=%d,", maxBacklog);
            } else if (maxBacklog > 1) {
                LOGGER.warn("The maxBacklog option is set in both the options String and the builder setMaxBacklog method. Ignoring setMaxBacklog");
            }

            commandOptions += options;
        } else {
            // Create options using fields set from builder
            
            if (skipN > 0) {
                commandOptions += String.format("skip_n=%d,", skipN);
            }

            if (maxBacklog > 1) {
                commandOptions += String.format("max_backlog=%d,", maxBacklog);
            }
        }
        if (headerKeys.contains(AMPSSourceHeaderKeys.TIMESTAMP) &&
                (!commandOptions.contains(Message.Options.Timestamp) &&
                 !commandOptions.contains("timestamp"))) {
            // Add timestamp to options if the header keys want a timestamp

            commandOptions = Message.Options.Timestamp + commandOptions;
        }
        if (!commandOptions.isEmpty()) {
            command.setOptions(commandOptions);
            LOGGER.debug("Set options '{}' for '{}' command", command.getOptions(), clientName);
        }

        // Bookmark from an AMPSSplit should only be used if the subscription is not a queue and the bookmark is not 
        // an empty string.
        if (queueSemantics == AMPSQueueSemantics.NONE && 
                readerCommand.split.getBookmark() != null && 
                !readerCommand.split.getBookmark().isEmpty()) {
            command.setBookmark(readerCommand.split.getBookmark());
        } else if (bookmark != null) {
            command.setBookmark(bookmark);
        }
        if (command.getBookmark() != null) {
            LOGGER.debug("Set bookmark '{}' for '{}' command", command.getBookmark(), clientName);
        }

        // Checks for ranged bookmark subscriptions, which will need an ack to know when to tell Flink that
        // the job is finished reading from AMPS.
        if (command.getBookmark() != null && (command.getBookmark().endsWith(")") || command.getBookmark().endsWith("]"))) {
            command.setAckType(Message.AckType.Completed);
            LOGGER.debug("Using a ranged bookmark for '{}' command", clientName);
        }

        // TopN needs an ack to tell Flink that the job is finished reading from AMPS.
        if (topN > 0) {
            command.setTopN(topN);
            LOGGER.debug("Set topN {} for '{}' command", command.getTopN(), clientName);

            // Only use completed ack type if the subscription should terminate after the topN.
            // If a topN field for the subscribe portion of sow_and_subscribe is added,
            // this will need to be updated.
            boolean isSowAndSub = subscribeCommand.equals("sow_and_subscribe") || subscribeCommand.equals("sow_and_delta_subscribe");
            if (!isSowAndSub) {
                command.setAckType(Message.AckType.Completed);
            }
        }

        command.setBatchSize(batchSize);
        LOGGER.debug("Set batchSize {} for '{}' command", command.getBatchSize(), clientName);
        
        if (orderBy != null) {
            command.setOrderBy(orderBy);
            LOGGER.debug("Set orderBy '{}' for '{}' command", command.getOrderBy(), clientName);
        }
        
        if (command.getCommand() == Message.Command.SOW) {
            command.setAckType(Message.AckType.Completed);
        }

        if (command.isSubscribe()) {
            command.setSubId(readerCommand.subId.toString());
        }

        try {
            readerCommand.commandId = client.executeAsync(command, new MessageHandler() {
                private final int index = subIndex;
                private final Field mhSubId = readerCommand.subId;

                @Override
                public void invoke(Message message) {
                    bytesFromAMPS.inc(message.isLengthNull() ? 0 : message.getDataRaw().length);

                    try {
                        Field field = message.getDataRaw();
                        byte[] data = new byte[field.length];
                        System.arraycopy(field.buffer, field.position, data, 0, field.length);
                        AMPSMessage msg = new AMPSMessage(data, charset);
                        
                        // Used when acking messages
                        if (queueSemantics != AMPSQueueSemantics.NONE || headerKeys.contains(AMPSSourceHeaderKeys.BOOKMARK)) {
                            field = message.getBookmarkRaw();
                            
                            if (field.buffer != null) {
                                data = new byte[field.length];
                                System.arraycopy(field.buffer, field.position, data, 0, field.length);
                                msg.setBookmark(data);
                            }
                        }

                        // Used when acking messages
                        if (queueSemantics != AMPSQueueSemantics.NONE || headerKeys.contains(AMPSSourceHeaderKeys.TOPIC)) {
                            field = message.getTopicRaw();
                            
                            if (field.buffer != null) {
                                data = new byte[field.length];
                                System.arraycopy(field.buffer, field.position, data, 0, field.length);
                                msg.setTopic(data);
                            }
                        }

                        // Used when discarding messages from a bookmark store
                        msg.setBookmarkSeqNo(message.getBookmarkSeqNo());
                        msg.setSubId(mhSubId);

                        if (includeTimestamps || headerKeys.contains(AMPSSourceHeaderKeys.TIMESTAMP)) {
                            field = message.getTimestampRaw();

                            if (field.buffer != null) {
                                data = new byte[field.length];
                                System.arraycopy(field.buffer, field.position, data, 0, field.length);
                                msg.setTimestamp(data);
                            }
                        }

                        // Used to help fetch() and the RecordEmitter
                        msg.setSplitIndex(index);

                        // Used to determine if the client is done receiving messages
                        msg.setAckType(message.getAckType());

                        // Used when determining whether or not to emit a message
                        msg.setCommand(message.getCommand());
                        
                        // Add additional headers
                        if (!headerKeys.isEmpty()) {
                            addHeaders(msg, message);
                        }
                        
                        messages.put(msg);

                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Add {} to buffer for '{}'", msg, clientName);
                        }
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Interrupted '{}' while trying to put message in queue", clientName);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        LOGGER.warn("Unexpected exception in '{}' message handler", clientName, e);
                        throw new RuntimeException(e);
                    }        
                }
            });
        } catch (AMPSException ae) {
            LOGGER.error("Failed to subscribe to AMPS for '{}'", clientName, ae);
            throw new RuntimeException(ae);
        }

        LOGGER.info("Added '{}' command to '{}'", subscribeCommand, clientName);
    }

    /**
     * Adds any additional {@link AMPSSourceHeaderKeys} to the {@link AMPSMessage}.
     *
     * Since an {@link AMPSMessage} already stores many of the headers as individual fields for internal use,
     * only some of the header keys need to be set here.
     *
     * @param ampsMessage The AMPS message to add the headers to.
     * @param message The message from AMPS to take the headers from.
     */
    protected void addHeaders(AMPSMessage ampsMessage, Message message) {
        if (headerKeys.contains(AMPSSourceHeaderKeys.SOW_KEY)) {
            ampsMessage.putHeader(AMPSSourceHeaderKeys.SOW_KEY, message.getSowKey());
        }
        if (headerKeys.contains(AMPSSourceHeaderKeys.CORRELATION_ID)) {
            ampsMessage.putHeader(AMPSSourceHeaderKeys.CORRELATION_ID, message.getCorrelationId());
        }
    }
    
    /**
     * Adds a list of splits to this reader.
     *
     * @param splits The splits to add.
     */
    protected void addSplits(List<AMPSSplit> splits) {
        LOGGER.debug("Adding {} new split(s) to '{}'", splits.size(), clientName);

        splits.forEach(split -> {
            ReaderCommand readerCommand = new ReaderCommand(split, commands.size(), null);
            StringField subId = readerCommand.subId;

            commands.add(readerCommand);

            if (useInternalBookmarkStore()) {
                try {
                    recoveryPointAdapter.init(new AMPSFlinkRP(subId, split.getBookmark()));
                } catch (Exception e) {
                    LOGGER.error("Failed to set up RecoveryPointAdapter for '{}'", clientName, e);
                    throw new RuntimeException(e);
                }
            }
        });

        if (useInternalBookmarkStore()) {
            LOGGER.debug("Set MemoryBookmarkStore and RecoveryPointAdapter for '{}'", clientName);

            try { 
                client.disconnect();

                // This bookmark store will always be a DefaultBookmarkStore or the MemoryBookmarkStore set by addSplits previously.
                client.getBookmarkStore().close();

                client.setBookmarkStore(new MemoryBookmarkStore(
                    commands.size(),
                    recoveryPointAdapter));

                client.connectAndLogon();
            } catch (Exception e) {
                LOGGER.error("Failed to set up MemoryBookmarkStore for '{}'", clientName, e);
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < commands.size(); i++) {
            if (commands.get(i).commandId == null) {
                // Only execute a command if not already subscribed
                executeCommand(commands.get(i), i);
            }
        }
    }
    
    /** 
     * Used to determine if a {@link MemoryBookmarkStore} with a {@link com.crankuptheamps.client.RecoveryPointAdapter} that uses Flink's checkpoints will be used. 
     */
    protected boolean useInternalBookmarkStore() {
        return deliveryGuarantee != DeliveryGuarantee.NONE && 
            (client.getBookmarkStore() instanceof DefaultBookmarkStore || bookmarkStore == null) && 
            queueSemantics == AMPSQueueSemantics.NONE;
    }

    /**
     * Class to keep track of the commands for an {@link com.crankuptheamps.flink.source.reader.AMPSSplitReader}.
     */
    @Internal
    protected static class ReaderCommand {
    
        /** The split used to set up the subscription. */
        public AMPSSplit split;
        /** The index of the subscription in the list used to track subscriptions. */
        public int subIndex;
        /** The subscription ID used for the subscription. */
        public StringField subId;
        /** The command ID. Is null when the command is paused or not created yet. */
        public CommandId commandId;
    
        public ReaderCommand(AMPSSplit split, int subIndex, CommandId commandId) {
            this.split = split;
            this.subIndex = subIndex;
            this.subId = new StringField("" + subIndex);
            this.commandId = commandId;
        }
    }
}

