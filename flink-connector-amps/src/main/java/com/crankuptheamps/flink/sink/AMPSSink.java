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

package com.crankuptheamps.flink.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.flink.sink.writer.AMPSStatefulSinkWriter;
import com.crankuptheamps.flink.sink.writer.AMPSWriterState;
import com.crankuptheamps.flink.sink.writer.AMPSWriterStateSerializer;
import com.crankuptheamps.flink.sink.writer.serializer.AMPSDataOnlySerializationSchemaWrapper;
import com.crankuptheamps.flink.sink.writer.serializer.AMPSSerializationSchema;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;
import com.crankuptheamps.flink.util.function.ExceptionListenerSupplier;
import com.crankuptheamps.flink.util.function.FailedWriteHandlerSupplier;
import com.crankuptheamps.flink.util.function.PublishStoreFunction;
import com.crankuptheamps.flink.util.function.ReconnectDelayStrategySupplier;
import com.crankuptheamps.flink.util.function.ServerChooserSupplier;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * {@link Sink} to publish messages to an AMPS topic.
 * 
 * <p>
 * This sink connects to an AMPS instance, serializes incoming records from Flink, and
 * publishes to AMPS.
 *
 * <p>
 * An example of a simple Flink job that will publish messages from Flink to AMPS is listed below.
 * This example would require a source to generate the data that is published to AMPS.
 *
 * <pre>
 * <code>
 *public static void main(String[] args) {
 *    AMPSSink<String> sink = AMPSSink.<String>builder()
 *        .setUri("tcp://ampsserver.example.com:9007/json")
 *        .setTopic("json-topic")
 *        .setSerializationSchema(new SimpleStringSchema())
 *        .build();
 *
 *    try {
 *        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *
 *        env.fromSource(unspecifiedSource, WatermarkStrategy.noWatermarks(), "Simple example")
 *            .sinkTo(sink);
 *        
 *        env.execute();
 *    } catch (Exception e) {
 *        e.printStackTrace();
 *    }
 *}
 * </code>
 * </pre>
 *
 * @param <IN> The input type of the sink.
 */
@PublicEvolving
public class AMPSSink<IN> implements Sink<IN>, SupportsWriterState<IN, AMPSWriterState> {

    private static final long serialVersionUID = 1L;

    /** The connection URI for the AMPS server. */
    protected final String uri;
    /** The AMPS topic to publish to. */
    protected final String topic;
    /** The name to identify the client. */
    protected final String clientName;
    /** The serializer for serializing messages sent to AMPS. */
    protected final AMPSSerializationSchema<IN> serializationSchema;
    /** The {@link java.util.function.Supplier} for getting a {@link com.crankuptheamps.client.ServerChooser}. */
    protected final ServerChooserSupplier serverChooserSupplier;
    /** The {@link com.crankuptheamps.client.util.SerializableFunction} for getting a {@link com.crankuptheamps.client.Store}. */
    protected final PublishStoreFunction publishStoreFunction;
    /** The publish command to use. */
    protected final String publishCommand;
    /** The delivery guarantee to use. */
    protected final DeliveryGuarantee guarantee;
    /** The {@link java.util.function.Supplier} for getting a {@link com.crankuptheamps.client.FailedWriteHandler}. */
    protected final FailedWriteHandlerSupplier failedWriteHandlerSupplier;
    /** The {@link java.util.function.Supplier} for getting a {@link com.crankuptheamps.client.ReconnectDelayStrategy}. */
    protected final ReconnectDelayStrategySupplier reconnectDelayStrategySupplier;
    /** The {@link java.util.function.Supplier} for getting an {@link java.beans.ExceptionListener}. */
    protected final ExceptionListenerSupplier exceptionListenerSupplier;
    /** The interval seconds for a heartbeat. */
    protected final int heartbeat;
    /** Flag for if the client should retry publishes if the client is disconnected. */
    protected final boolean isRetryOnDisconnect;
    /** The expiration for the SOW/queue message. */
    protected final int expiration;
    /** Flag for if a suffix should be added to the end of the client name to ensure unique names. */
    protected final boolean useSuffix;
    /** The milliseconds to wait for messages to be sent and processed by the AMPS server. */
    protected final long flushTimeout;
    /** The correlation ID to use for all messages published by the sink. */
    protected final String correlationId;
    /** The functional interface that will be called before the {@link HAClient} connects to AMPS. */
    protected final ConnectorInitializer initCallback;

    /**
     * Constructor to create an {@link AMPSSink}. 
     *
     * Use the {@link #builder()} to construct instances.
     */
    protected AMPSSink(String uri, String topic, String clientName, AMPSSerializationSchema<IN> serializationSchema, 
            ServerChooserSupplier serverChooserSupplier, PublishStoreFunction publishStoreFunction, String publishCommand,
            DeliveryGuarantee guarantee, FailedWriteHandlerSupplier failedWriteHandlerSupplier, ReconnectDelayStrategySupplier reconnectDelayStrategySupplier,
            ExceptionListenerSupplier exceptionListenerSupplier, int heartbeat, boolean isRetryOnDisconnect, int expiration,
            boolean useSuffix, long flushTimeout, String correlationId, ConnectorInitializer initCallback) {
        this.uri = uri;
        this.topic = topic;
        this.clientName = clientName;
        this.serializationSchema = serializationSchema;
        this.serverChooserSupplier = serverChooserSupplier;
        this.publishStoreFunction = publishStoreFunction;
        this.publishCommand = publishCommand;
        this.guarantee = guarantee;
        this.failedWriteHandlerSupplier = failedWriteHandlerSupplier;
        this.reconnectDelayStrategySupplier = reconnectDelayStrategySupplier;
        this.exceptionListenerSupplier = exceptionListenerSupplier;
        this.heartbeat = heartbeat;
        this.isRetryOnDisconnect = isRetryOnDisconnect;
        this.expiration = expiration;
        this.useSuffix = useSuffix;
        this.flushTimeout = flushTimeout;
        this.correlationId = correlationId;
        this.initCallback = initCallback;
    }

    /**
     * Creates a {@link SinkWriter} that will set up and connect the AMPS client.
     */
    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return createWriterWithClient(context, new ArrayList<>(), new HAClient());
    }

    /**
     * Creates an {@link AMPSStatefulSinkWriter} from a recovered state.
     *
     * @param context The runtime context.
     * @param recoveredState The state to recover from.
     * @throws IOException On failure during creation.
     */
    @Override
    public StatefulSinkWriter<IN, AMPSWriterState> restoreWriter(WriterInitContext context, Collection<AMPSWriterState> recoveredState) {
        return createWriterWithClient(context, recoveredState, new HAClient());
    }
    
    /**
     * Creates a new {@link AMPSStatefulSinkWriter} using the {@link HAClient} provided by the parameters.
     *
     * @param context The runtime context.
     * @param recoveredState The state to recover from.
     * @param client The HAClient that the writer will use.
     * @throws IOException On failure during creation.
     */
    @VisibleForTesting
    public StatefulSinkWriter<IN, AMPSWriterState> createWriterWithClient(WriterInitContext context, 
            Collection<AMPSWriterState> recoveredState, HAClient client) {
        return new AMPSStatefulSinkWriter<>(
            context,
            topic, 
            serializationSchema,
            uri,
            clientName,
            serverChooserSupplier == null ? null : serverChooserSupplier.get(),
            publishStoreFunction,
            publishCommand,
            guarantee,
            recoveredState,
            failedWriteHandlerSupplier == null ? null : failedWriteHandlerSupplier.get(),
            reconnectDelayStrategySupplier == null ? null : reconnectDelayStrategySupplier.get(),
            exceptionListenerSupplier == null ? null : exceptionListenerSupplier.get(),
            heartbeat,
            isRetryOnDisconnect,
            expiration,
            client,
            useSuffix || context.getTaskInfo().getNumberOfParallelSubtasks() > 1,
            flushTimeout,
            correlationId,
            initCallback);
    }

    /**
     * Gets a serializer for the {@link AMPSWriterState} class.
     *
     * @return A new SimpleVersionedSerializer.
     */
    @Override
    public SimpleVersionedSerializer<AMPSWriterState> getWriterStateSerializer() {
        return new AMPSWriterStateSerializer();
    }

    /**
     * Creates a new {@link AMPSSinkBuilder} to construct an {@link AMPSSink}.
     * 
     * <p>
     * The following are required to create a valid {@link AMPSSink}:
     * <ul>
     *  <li>A uri or {@link ServerChooserSupplier}</li>
     *  <li>A topic</li>
     *  <li>A {@link SerializationSchema} or an {@link AMPSSerializationSchema}</li>
     * </ul>
     *
     * @param <IN> The type of elements consumed by the sink.
     * @return A new AMPSSinkBuilder.
     */
    public static <IN> AMPSSinkBuilder<IN> builder() {
        return new AMPSSinkBuilder<>();
    }

    /**
     * A builder for creating {@link AMPSSink} instances.
     *
     * <p>
     * The following are required to create a valid {@link AMPSSink}:
     * <ul>
     *  <li>A uri or {@link ServerChooserSupplier}</li>
     *  <li>A topic</li>
     *  <li>A {@link SerializationSchema} or an {@link AMPSSerializationSchema}</li>
     * </ul>
     *
     * @param <IN> The type of elements consumed by the sink.
     */
    @PublicEvolving
    public static class AMPSSinkBuilder<IN> {
        protected String uri;
        protected String topic;
        protected String clientName = "AMPSSink";
        protected AMPSSerializationSchema<IN> serializationSchema;
        protected ServerChooserSupplier serverChooserSupplier;
        protected PublishStoreFunction publishStoreFunction;
        protected String publishCommand = "publish";
        protected DeliveryGuarantee guarantee = DeliveryGuarantee.NONE;
        protected FailedWriteHandlerSupplier failedWriteHandlerSupplier;
        protected ReconnectDelayStrategySupplier reconnectDelayStrategySupplier;
        protected ExceptionListenerSupplier exceptionListenerSupplier;
        protected int heartbeat = 0;
        protected boolean isRetryOnDisconnect = true;
        protected int expiration = 0;
        protected boolean useSuffix = false;
        protected long flushTimeout = 10_000L;
        protected String correlationId;
        protected ConnectorInitializer initCallback;

        /**
         * Sets the connection URI for the AMPS server (e.g., "tcp://localhost:9007/amps/json").
         *
         * @param uri The AMPS connection URI.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the AMPS topic.
         *
         * @param topic The name of the topic.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Sets the client name to use when connecting to AMPS.
         *
         * @param clientName The client name.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setClientName(String clientName) {
            this.clientName = clientName;
            return this;
        }

        /**
         * Sets the {@link AMPSSerializationSchema} for serializing records of type IN.
         *
         * @param serializationSchema The {@link AMPSSerializationSchema}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setSerializationSchema(AMPSSerializationSchema<IN> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }
        
        /**
         * Sets the {@link AMPSSerializationSchema} for serializing records of type IN.
         *
         * This method takes a {@link SerializationSchema} and creates an {@link AMPSDataOnlySerializationSchemaWrapper}
         * to serialize elements from Flink to the input type.
         *
         * @param serializationSchema The {@link SerializationSchema}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setSerializationSchema(SerializationSchema<IN> serializationSchema) {
            this.serializationSchema = new AMPSDataOnlySerializationSchemaWrapper<>(serializationSchema);
            return this;
        }

        /**
         * Sets the {@link ServerChooserSupplier} for supplying a {@link com.crankuptheamps.client.ServerChooser} to the writers.
         *
         * @param serverChooserSupplier The {@link ServerChooserSupplier}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setServerChooserSupplier(ServerChooserSupplier serverChooserSupplier) {
            this.serverChooserSupplier = serverChooserSupplier;
            return this;
        }

        /**
         * Sets the {@link PublishStoreFunction} for supplying a {@link com.crankuptheamps.client.Store} to the writers.
         *
         * This can be used to provide some guarantees when publishing to AMPS, but the sink's publishes will not
         * be tied to Flink checkpoints unless both checkpointing is enabled and the {@link org.apache.flink.connector.base.DeliveryGuarantee} is set.
         *
         * @param publishStoreFunction The {@link PublishStoreFunction}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setPublishStoreFunction(PublishStoreFunction publishStoreFunction) {
            this.publishStoreFunction = publishStoreFunction;
            return this;
        }

        /**
         * Sets the publish command. 
         *
         * Default is "publish".
         *
         * <p>
         * Supported commands include:
         * <ul>
         *  <li>"publish"</li>
         *  <li>"delta_publish"</li>
         *  <li>"sow_delete"</li>
         * </ul>
         *
         * <p>
         * The command "delta_publish" may require a custom {@link SerializationSchema} to ensure
         * only the intended fields are updated.
         *
         * @param publishCommand The publish command.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setPublishCommand(String publishCommand) {
            if (publishCommand.equals("publish") || 
                publishCommand.equals("delta_publish") ||
                publishCommand.equals("sow_delete")) {
                this.publishCommand = publishCommand;
            } else {
                throw new UnsupportedOperationException("AMPSSink does not support publish command '" + publishCommand + "'.");
            }
            return this;
        }

        /**
         * Sets the {@link DeliveryGuarantee} for the sink. 
         *
         * <p>
         * When set to DeliveryGuarantee.AT_LEAST_ONCE or DeliveryGuarantee.EXACTLY_ONCE,
         * a {@link com.crankuptheamps.client.Store} is required.
         * By default, a {@link com.crankuptheamps.client.MemoryPublishStore} is used.
         *
         * <p>
         * Both DeliveryGuarantee.AT_LEAST_ONCE and DeliveryGuarantee.EXACTLY_ONCE make the sink
         * store incoming records rather than publish them immediately. On checkpoint completion,
         * the sink flushes the stored records by publishing them all at once to AMPS.
         * This means that checkpointing must be enabled for either guarantee.
         * 
         * <p>
         * On recovery, DeliveryGuarantee.EXACTLY_ONCE uses timestamps to avoid
         * republishing messages that arrived before the last published message. Timestamps
         * are required to be strictly increasing. If this condition cannot be
         * met, use DeliveryGuarantee.AT_LEAST_ONCE.
         *
         * <p>
         * DeliveryGuarantee.AT_LEAST_ONCE should be used over DeliveryGuarantee.EXACTLY_ONCE in
         * message queues due to how the writer recovers on failure.
         *
         * @param guarantee The {@link DeliveryGuarantee}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee guarantee) {
            this.guarantee = guarantee;
            return this;
        }

        /**
         * Sets the {@link FailedWriteHandlerSupplier} for supplying a {@link com.crankuptheamps.client.FailedWriteHandler} to the writers.
         *
         * @param failedWriteHandlerSupplier The {@link FailedWriteHandlerSupplier}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setFailedWriteHandlerSupplier(FailedWriteHandlerSupplier failedWriteHandlerSupplier) {
            this.failedWriteHandlerSupplier = failedWriteHandlerSupplier;
            return this;
        }

        /**
         * Sets the {@link ReconnectDelayStrategySupplier} for the clients.
         *
         * @param reconnectDelayStrategySupplier The {@link ReconnectDelayStrategySupplier}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setReconnectDelayStrategySupplier(ReconnectDelayStrategySupplier reconnectDelayStrategySupplier) {
            this.reconnectDelayStrategySupplier = reconnectDelayStrategySupplier;
            return this;
        }

        /**
         * Sets the {@link ExceptionListenerSupplier} for the subscription, allowing exceptions that are absorbed by the AMPS client to be communicated.
         *
         * @param exceptionListenerSupplier The {@link ExceptionListenerSupplier}.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setExceptionListenerSupplier(ExceptionListenerSupplier exceptionListenerSupplier) {
            this.exceptionListenerSupplier = exceptionListenerSupplier;
            return this;
        }
        
        /**
         * Sets the heartbeat for the client.
         *
         * @param intervalSeconds The seconds between beats from the server.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setHeartbeat(int intervalSeconds) {
            this.heartbeat = intervalSeconds;
            return this;
        }

        /**
         * Sets whether or not messages being sent to the server should retry if the client is disconnected.
         *
         * This is most useful if you are publishing data that has a very short lifetime and may no longer be
         * relevant after the time it takes to reconnect.
         *
         * @param isRetryOnDisconnect False to disable default behavior of automatic retry.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setRetryOnDisconnect(boolean isRetryOnDisconnect) {
            this.isRetryOnDisconnect = isRetryOnDisconnect;
            return this;
        }
        
        /**
         * Sets the expiration for SOW/queue messages from the client.
         *
         * @param expiration The amount of time to retain the message.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setExpiration(int expiration) {
            this.expiration = expiration;
            return this;
        }
        
        /**
         * Sets the flag for if a suffix should be appended to the client name.
         *
         * Default value is false, but if parallelism is greater than 1, it will always be true.
         *
         * <p>
         * The suffix is used to ensure unique client names among writers.
         * Each client needs a unique client name,
         * so this field ensures that each client receives a unique client name.
         *
         * <p>
         * The suffix is the same for an individual job submission even in the event of failures, but it will
         * be different across several submissions. For example, if job A is submitted and finishes, submitting
         * job A again will result in different suffixes compared to the first submission.
         *
         * @param useSuffix Flag for if a suffix is appended to the client name.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setUseSuffix(boolean useSuffix) {
            this.useSuffix = useSuffix;
            return this;
        }

        /**
         * Sets the max duration clients will wait for a publish flush.
         *
         * Default value is 10_000L or 10 seconds.
         *
         * @param flushTimeout The amount of milliseconds a client will wait for the flush.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setFlushTimeout(long flushTimeout) {
            this.flushTimeout = flushTimeout;
            return this;
        }
        
        /**
         * Sets the correlation ID to use for all messages published by this sink.
         *
         * The correlation ID must only contain characters that are valid base64 encoded characters.
         * This value is overridden by any correlation ID set by an {@link AMPSSerializationSchema}.
         *
         * @param correlationId The correlation ID to use.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        /**
         * Sets the functional interface that will be called before the {@link HAClient} connects
         * to AMPS.
         *
         * @param initCallback The {@link ConnectorInitializer} that will be called.
         * @return This builder.
         */
        public AMPSSinkBuilder<IN> setConnectorInitializer(ConnectorInitializer initCallback) {
            this.initCallback = initCallback;
            return this;
        }
        
        /**
         * Builds the {@link AMPSSink} with the configured properties.
         *
         * @return A new {@link AMPSSink} instance.
         */
        public AMPSSink<IN> build() {
            return new AMPSSink<>(uri, topic, clientName, serializationSchema, 
                serverChooserSupplier, publishStoreFunction, publishCommand, guarantee,
                failedWriteHandlerSupplier, reconnectDelayStrategySupplier, exceptionListenerSupplier,
                heartbeat, isRetryOnDisconnect, expiration, useSuffix, flushTimeout, correlationId, initCallback);
        }
    }
}

