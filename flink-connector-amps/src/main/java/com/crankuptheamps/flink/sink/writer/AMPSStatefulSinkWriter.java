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

package com.crankuptheamps.flink.sink.writer;

import java.beans.ExceptionListener;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.FailedWriteHandler;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.MemoryPublishStore;
import com.crankuptheamps.client.ReconnectDelayStrategy;
import com.crankuptheamps.client.ServerChooser;
import com.crankuptheamps.client.Store;
import com.crankuptheamps.flink.sink.metrics.SinkMetrics;
import com.crankuptheamps.flink.sink.writer.serializer.AMPSSerializationSchema;
import com.crankuptheamps.flink.util.ClientNamePool;
import com.crankuptheamps.flink.util.SerializedElement;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;
import com.crankuptheamps.flink.util.function.PublishStoreFunction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.apache.flink.api.connector.sink2.StatefulSinkWriter} to publish messages to an AMPS topic.
 * 
 * This sink writer connects to an AMPS instance and publishes records from a Flink stream
 * to a specified topic after serializing them.
 * 
 * @param <IN> The input type of the sink.
 */
@Internal
public class AMPSStatefulSinkWriter<IN> implements StatefulSinkWriter<IN, AMPSWriterState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMPSStatefulSinkWriter.class);
    /** Map of client names to use. */
    private static final Map<String, ClientNamePool> clientNames = new HashMap<>();

    /** The AMPS High-Availability client instance. */
    protected final HAClient client;
    /** The context for the writer. */
    protected final WriterInitContext context;
    /** The connection URI for the AMPS server. */
    protected final String uri;
    /** The AMPS topic to publish to. */
    protected final String topic;
    /** The original client name without the suffix. */
    protected final String originalName;
    /** The name to identify this client in AMPS. */
    protected final String clientName;
    /** The schema to serialize messages for AMPS. */
    protected final AMPSSerializationSchema<IN> serializationSchema;
    /** The server chooser for this client. */
    protected final ServerChooser serverChooser;
    /** The publish store for this client. */
    protected final Store publishStore;
    /** Byte array of the topic. */
    protected final byte[] topicBytes;
    /** The publish command for this client. */
    protected final String publishCommand;
    /** The delivery guarantee for the client. */
    protected final DeliveryGuarantee guarantee;
    /** The failed write handler for this client. */
    protected final FailedWriteHandler failedWriteHandler;
    /** The reconnect delay strategy for this client. */
    protected final ReconnectDelayStrategy reconnectDelayStrategy;
    /** The exception listener for this client. */
    protected final ExceptionListener exceptionListener;
    /** The heartbeat for this client. */
    protected final int heartbeat;
    /** Flag for whether or not messages being sent to the server should retry after a disconnect. */
    protected final boolean isRetryOnDisconnect;
    /** The amount of time for a SOW/queue message before it expires. */
    protected final int expiration;
    /** The recovered state. */
    protected final List<AMPSWriterState> recoveredState;
    /** Counter for connects. */
    protected final Counter connects;
    /** Counter for disconnects. */
    protected final Counter disconnects;
    /** Records send to AMPS. */
    protected final Counter recordsSent;
    /** The bytes send to AMPS. */
    protected final Counter bytesSent;
    /** Records failed to send to AMPS. */
    protected final Counter numRecordsOutErrors;
    /** Records failed to send to AMPS. */
    protected final Counter numRecordsSendErrors;
    /** Command for publishing to AMPS. */
    protected final Command command;
    /** {@link com.crankuptheamps.client.Message.Command} for the command. */
    protected final int commandType;
    /** Flag for if expiration should be set. */
    protected final boolean setExpiration;
    /** Milliseconds to wait for a publish flush. */
    protected final long flushTimeout;
    /** The correlation ID to use on all messages. */
    protected final String correlationId;
    /** The functional interface that will be called before an {@link HAClient} connects to AMPS. */
    protected final ConnectorInitializer initCallback;

    /** The list of messages that need to be published. */
    protected List<SerializedElement> pendingMessages = new ArrayList<>();
    /** The last timestamp of a message that was added to the list. */
    protected Long lastTimestamp = Long.MIN_VALUE;
    /** Flag for if the state has been recovered. */
    protected boolean recovered;

    /**
     * Constructor for an {@link AMPSStatefulSinkWriter}. 
     */
    public AMPSStatefulSinkWriter(WriterInitContext context, String topic, AMPSSerializationSchema<IN> serializationSchema, String uri, String clientName, 
            ServerChooser serverChooser, PublishStoreFunction publishStoreFunction, String publishCommand, DeliveryGuarantee guarantee, Collection<AMPSWriterState> recoveredState,
            FailedWriteHandler failedWriteHandler, ReconnectDelayStrategy reconnectDelayStrategy, ExceptionListener exceptionListener, int heartbeat,
            boolean isRetryOnDisconnect, int expiration, HAClient client, boolean useSuffix, long flushTimeout, String correlationId, ConnectorInitializer initCallback) {
        this.context = context;
        this.uri = uri;
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
        
        this.topic = topic;
        this.serializationSchema = serializationSchema;
        this.serverChooser = serverChooser;
        
        if (publishStoreFunction != null) {
            synchronized (publishStoreFunction) {
                this.publishStore = publishStoreFunction.apply(this.clientName);
            }
        } else {
            this.publishStore = null;
        }
        
        this.publishCommand = publishCommand;
        this.guarantee = guarantee;
        this.recoveredState = new ArrayList<>(recoveredState);
        this.recovered = recoveredState.isEmpty();
        this.failedWriteHandler = failedWriteHandler;
        this.reconnectDelayStrategy = reconnectDelayStrategy;
        this.exceptionListener = exceptionListener;
        this.heartbeat = heartbeat;
        this.isRetryOnDisconnect = isRetryOnDisconnect;
        this.expiration = expiration;
        this.client = client;

        if (topic == null) {
            LOGGER.error("Topic cannot be null");
            throw new RuntimeException("Topic cannot be null");
        }

        if (!recoveredState.isEmpty()) {
            LOGGER.info("Restoring with {}", recoveredState);
        }

        this.topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        this.command = new Command(publishCommand);
        this.commandType = command.getCommand();
        this.setExpiration = expiration > 0;
        this.flushTimeout = flushTimeout;
        this.correlationId = correlationId;
        this.initCallback = initCallback;

        this.connects = context.metricGroup()
            .addGroup(SinkMetrics.Group.SINK)
            .addGroup(SinkMetrics.Group.WRITER)
            .counter(SinkMetrics.Metric.CONNECTS);

        this.disconnects = context.metricGroup()
            .addGroup(SinkMetrics.Group.SINK)
            .addGroup(SinkMetrics.Group.WRITER)
            .counter(SinkMetrics.Metric.DISCONNECTS);

        this.recordsSent = context.metricGroup().getNumRecordsSendCounter();
        this.bytesSent = context.metricGroup().getNumBytesSendCounter();
        this.numRecordsOutErrors = context.metricGroup().getNumRecordsOutErrorsCounter();
        this.numRecordsSendErrors = context.metricGroup().getNumRecordsSendErrorsCounter();
        
        try {
            startClient();
        } catch (Exception ex) {
            LOGGER.error("Failed to start client", ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Closes the sink writer, disconnecting the AMPS client.
     *
     * @throws Exception If the client cannot be closed.
     */
    @Override
    public void close() throws Exception {
        LOGGER.info("Closing client '{}'", clientName);

        try {
            client.publishFlush(flushTimeout);
        } catch (Exception e) {
            LOGGER.error("Failed to publish flush for '{}'", clientName, e);
        }

        client.close();

        try (Store store = client.getPublishStore();) {
            if (store != null) {
                store.close();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close publish store for '{}'", clientName, e);
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
     * Called for each record in the stream. 
     *
     * Serializes the record, and if the delivery guarantee is NONE,
     * immediately publishes it to the configured AMPS topic.
     *
     * @param element The input record.
     * @param context The context of the sink.
     */
    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Write {} from '{}'", element, clientName);
        }

        SerializedElement serializedElement = serializationSchema.serialize(element, context);

        switch (guarantee) {
            case NONE -> {
                sendMessage(serializedElement);
            }
            case AT_LEAST_ONCE -> {
                pendingMessages.add(serializedElement);
            }
            case EXACTLY_ONCE -> {
                Long elementTimestamp = context.timestamp();

                if (elementTimestamp == null || elementTimestamp < 0 || elementTimestamp <= lastTimestamp) {
                    String err = "Exactly once delivery for an AMPSSink requires strictly increasing " +
                        "timestamps. Received timestamp: " + elementTimestamp + ", " +
                        "last timestamp: " + lastTimestamp;
                    LOGGER.error(err);
                    throw new UnsupportedOperationException(err);
                }

                if (!recovered) {
                    if (recoveredState.size() != 1) {
                        String err = "Recovered state should have 1 stored state but has " + recoveredState.size();
                        LOGGER.error(err);
                        throw new IllegalStateException(err);
                    }
                    
                    AMPSWriterState ws = recoveredState.get(0);

                    // Check if message was published already
                    if (elementTimestamp <= ws.getLastTimestamp()) break;

                    LOGGER.debug("Successful recovery for '{}'", clientName);
                    recovered = true;
                }

                pendingMessages.add(serializedElement);
                lastTimestamp = elementTimestamp;
            }
        }
    }

    /**
     * Called to flush all pending messages to AMPS.
     *
     * @param endOfInput Flag for if the data stream has reached the end of input.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        List<SerializedElement> sendingMessages = pendingMessages;
        LOGGER.debug("Flushing {} message(s) from '{}'", sendingMessages.size(), clientName);
        pendingMessages = new ArrayList<>();

        for (SerializedElement message : sendingMessages) {
            sendMessage(message);
        }

        try {    
            client.publishFlush(flushTimeout);

            if (endOfInput) {
                LOGGER.info("Received end of input for '{}'", clientName);
            } 
        }
        catch (Exception e) {
            LOGGER.error("Failed to publish flush for '{}'", clientName, e);
            throw new IOException(e);
        }     
    }

    /**
     * Snapshots the state of the writer using the timestamp of the last written message.
     *
     * @param checkpointId The checkpoint ID.
     * @return A list that contains a single AMPSWriterState of the snapshotted state.
     */
    @Override
    public List<AMPSWriterState> snapshotState(long checkpointId) throws IOException {
        List<AMPSWriterState> state = new ArrayList<>();

        if (!recovered && lastTimestamp < 0) {
            // Reuse the recovered state if a failure occurs before a snapshot
            state.add(recoveredState.get(0));
        } else {
            state.add(new AMPSWriterState(lastTimestamp));
        }
        
        LOGGER.debug("Created new writer state {}", state.get(0));

        return state;
    }

    /** 
     * Starts the client.
     *
     * @throws Exception If the client has an issue while starting.
     */
    private void startClient() throws Exception {
        LOGGER.info("Starting client '{}'", clientName);

        serializationSchema.open(context.asSerializationSchemaInitializationContext());

        if (serverChooser != null) {
            LOGGER.debug("Set server chooser");
            client.setServerChooser(serverChooser);
        } else {
            LOGGER.debug("Create and set server chooser using URI");
            DefaultServerChooser defaultServerChooser = new DefaultServerChooser();
            defaultServerChooser.add(uri);
            client.setServerChooser(defaultServerChooser);
        }

        if (publishStore != null) {
            LOGGER.debug("Set publish store");
            client.setPublishStore(publishStore);
        } else if (guarantee != DeliveryGuarantee.NONE) {
            LOGGER.debug("Create and set MemoryPublishStore");
            client.setPublishStore(new MemoryPublishStore(1000));
        }

        if (failedWriteHandler != null) {
            LOGGER.debug("Set failed writer handler");
            client.setFailedWriteHandler(failedWriteHandler);
        }

        if (reconnectDelayStrategy != null) {
            LOGGER.debug("Set reconnect delay strategy");
            client.setReconnectDelayStrategy(reconnectDelayStrategy);
        }
        
        if (exceptionListener != null) {
            LOGGER.debug("Set exception listener");
            client.setExceptionListener(exceptionListener);
        }

        if (heartbeat > 0) {
            LOGGER.debug("Set heartbeat with interval: {}", heartbeat);
            client.setHeartbeat(heartbeat);
        }

        LOGGER.debug("Set retry on disconnect: {}", isRetryOnDisconnect);
        client.setRetryOnDisconnect(isRetryOnDisconnect);

        LOGGER.debug("Set topic '{}'", topic);
        LOGGER.debug("Set command '{}'", publishCommand);
        
        if (setExpiration) {
            LOGGER.debug("Set expiration {}", expiration);
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

        LOGGER.debug("Set name to '{}'", clientName);
        client.setName(clientName);
        
        if (initCallback != null) {
            LOGGER.debug("Entering provided connector initializer {}", initCallback);
            initCallback.init(client);
            LOGGER.debug("Exited provided connector initializer {}", initCallback);
        }

        if (client.getTransport() == null) {
            LOGGER.debug("Connect and logon client '{}'", clientName);
            client.connectAndLogon();
        }

        LOGGER.info("Started client '{}'", client.getName());
    }

    /**
     * Publishes a message to AMPS.
     *
     * @param message The serialized message along with additional data.
     * @throws IOException Exception while publishing to AMPS.
     */
    protected void sendMessage(SerializedElement message) throws IOException {
        try {
            command.reset(commandType);
    
            command.setTopic(topicBytes, 0, topicBytes.length);

            if (!message.isDataNull()) {
                byte[] data = message.getData();
                command.setData(data, 0, data.length);
                bytesSent.inc(data.length);
            }
            
            if (setExpiration) {
                command.setExpiration(expiration);
            }

            if (!message.isCorrelationIdNull()) {
                command.setCorrelationId(message.getCorrelationId());
            } else if (correlationId != null) {
                command.setCorrelationId(correlationId);
            }

            if (!message.isSowKeyNull()) {
                command.setSowKey(message.getSowKey());
            }
                    
            client.executeAsync(command, null);

            recordsSent.inc();
        } catch (Exception e) {
            numRecordsOutErrors.inc();
            numRecordsSendErrors.inc();
            LOGGER.error("Failed '{}' command for '{}'", publishCommand, clientName, e);
            throw new IOException(e);
        }
    }

    /**
     * Returns the client used by this writer.
     *
     * This should only be used during testing.
     *
     * @return The client used by this writer.
     */
    @VisibleForTesting
    public HAClient getClient() {
        return client;
    }
}

