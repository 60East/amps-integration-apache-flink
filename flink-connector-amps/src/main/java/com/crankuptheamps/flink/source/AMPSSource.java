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

package com.crankuptheamps.flink.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.flink.source.checkpointing.AMPSCheckpoint;
import com.crankuptheamps.flink.source.checkpointing.AMPSCheckpointSerializer;
import com.crankuptheamps.flink.source.reader.AMPSSourceReader;
import com.crankuptheamps.flink.source.reader.deserializer.AMPSDataOnlyDeserializationSchemaWrapper;
import com.crankuptheamps.flink.source.reader.deserializer.AMPSDeserializationSchema;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.source.split.AMPSSplitEnumerator;
import com.crankuptheamps.flink.source.split.AMPSSplitSerializer;
import com.crankuptheamps.flink.util.AMPSQueueSemantics;
import com.crankuptheamps.flink.util.AMPSSourceHeaderKeys;
import com.crankuptheamps.flink.util.function.BookmarkStoreFunction;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;
import com.crankuptheamps.flink.util.function.ExceptionListenerSupplier;
import com.crankuptheamps.flink.util.function.ReconnectDelayStrategySupplier;
import com.crankuptheamps.flink.util.function.ServerChooserSupplier;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * {@link Source} to consume messages from an AMPS topic using the FLIP-27 Data Source API.
 * 
 * <p>
 * This source connects to an AMPS instance, subscribes to or queries a specified topic,
 * deserializes incoming messages from AMPS, and emits them to Flink.
 *
 * <p>
 * An example of a simple Flink job that will print messages from an ad hoc topic from AMPS
 * to stdout is listed below. This example would require a publisher for the ad hoc topic
 * to see the published messages being printed to stdout.
 *
 * <pre>
 * <code>
 *public static void main(String[] args) {
 *    AMPSSource<String> source = AMPSSource.<String>builder()
 *        .setUri("tcp://ampsserver.example.com:9007/json")
 *        .setTopic("json-topic")
 *        .setDeserializationSchema(new SimpleStringSchema())
 *        .build();
 *
 *    try {
 *        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *
 *        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Simple example")
 *            .print();
 *        
 *        env.execute();
 *    } catch (Exception e) {
 *        e.printStackTrace();
 *    }
 *}
 * </code>
 * </pre>
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public class AMPSSource<OUT> implements Source<OUT, AMPSSplit, AMPSCheckpoint>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    /** The connection URI for the AMPS server. */
    protected final String uri;
    /** The AMPS topic to subscribe to. */
    protected final String topic;
    /** The name to identify the client. */
    protected final String clientName;
    /** The content filter for the subscription. */
    protected final String contentFilter;
    /** Additional options for the subscription command. */
    protected final String options;
    /** The deserializer for deserializing messages from AMPS. */
    protected final AMPSDeserializationSchema<OUT> deserializationSchema;
    /** The {@link com.crankuptheamps.client.util.SerializableFunction} for getting a {@link com.crankuptheamps.client.BookmarkStore}. */
    protected final BookmarkStoreFunction bookmarkStoreFunction;
    /** The {@link java.util.function.Supplier} for getting a {@link com.crankuptheamps.client.ServerChooser}. */
    protected final ServerChooserSupplier serverChooserSupplier;
    /** The starting bookmark for the subscription. */
    protected final String bookmark;
    /** The topics, content filters, and bookmarks that will act as the splits. */
    protected final List<AMPSSplit> splits;
    /** The semantics for the queue. */
    protected final AMPSQueueSemantics queueSemantics;
    /** The ack batch size of messages when auto ack is enabled. */
    protected final int ackBatchSize;
    /** The ack timeout for messages when auto ack is enabled. */
    protected final int ackTimeout;
    /** The amount of messages to read from AMPS. */
    protected final int topN;
    /** The delivery guarantee for the source. */
    protected final DeliveryGuarantee deliveryGuarantee;
    /** The subscribe command used by the client. */
    protected final String subscribeCommand;
    /** The {@link java.util.function.Supplier} for getting an {@link java.beans.ExceptionListener}. */
    protected final ExceptionListenerSupplier exceptionListenerSupplier;
    /** The size of the internal buffer for the client. */
    protected final int internalBufferSize;
    /** The batch size that the client will use for its command. */
    protected final int batchSize;
    /** The String of how the results of a SOW query should be ordered. */
    protected final String orderBy;
    /** The {@link java.util.function.Supplier} for getting a {@link com.crankuptheamps.client.ReconnectDelayStrategy}. */
    protected final ReconnectDelayStrategySupplier reconnectDelayStrategySupplier;
    /** The heartbeat interval for the client. */
    protected final int heartbeat;
    /** The configuration for storing {@link org.apache.flink.connector.base.source.reader.SourceReaderOptions}. */
    protected final Configuration config;
    /** The amount of messages to skip from AMPS. */
    protected final int skipN;
    /** The max backlog of messages from a queue. */
    protected final int maxBacklog;
    /** Flag for if a number should be added to the end of the client name to ensure unique names. */
    protected final boolean useSuffix;
    /** Interval in milliseconds for when to prune a {@link com.crankuptheamps.client.LoggedBookmarkStore}. */
    protected final long pruneInterval;
    /** The milliseconds to sleep for after blocking for messages. */
    protected final int sleepMillisAfterBlock;
    /** Flag for if bookmarks are discarded immediately after emitting records. */
    protected final boolean discardAfterEmit;
    /** The set of header keys that will be preserved from the {@link com.crankuptheamps.client.Message}. */
    protected final Set<AMPSSourceHeaderKeys> headerKeys;
    /** The functional interface that will be called before the {@link HAClient} connects to AMPS. */
    protected final ConnectorInitializer initCallback;

    /**
     * Constructor to create an {@link AMPSSource}. 
     *
     * Use the {@link #builder()} to construct instances.
     */
    protected AMPSSource(String uri, String topic, String clientName, String contentFilter, String options,
                           AMPSDeserializationSchema<OUT> deserializationSchema, BookmarkStoreFunction bookmarkStoreFunction,
                           ServerChooserSupplier serverChooserSupplier, String bookmark, List<AMPSSplit> splits, AMPSQueueSemantics queueSemantics,
                           int ackBatchSize, int ackTimeout, int topN, DeliveryGuarantee deliveryGuarantee, String subscribeCommand, ExceptionListenerSupplier exceptionListenerSupplier,
                           int internalBufferSize, int batchSize, String orderBy, ReconnectDelayStrategySupplier reconnectDelayStrategySupplier,
                           int heartbeat, Configuration config, int skipN, int maxBacklog, boolean useSuffix, long pruneInterval,
                           int sleepMillisAfterBlock, boolean discardAfterEmit, Set<AMPSSourceHeaderKeys> headerKeys,
                           ConnectorInitializer initCallback) {
        this.uri = uri;
        this.topic = topic;
        this.clientName = clientName;
        this.contentFilter = contentFilter;
        this.options = options;
        this.deserializationSchema = deserializationSchema;
        this.bookmarkStoreFunction = bookmarkStoreFunction;
        this.serverChooserSupplier = serverChooserSupplier;
        this.bookmark = bookmark;
        this.splits = splits;
        this.queueSemantics = queueSemantics;
        this.ackBatchSize = ackBatchSize;
        this.ackTimeout = ackTimeout;
        this.topN = topN;
        this.deliveryGuarantee = deliveryGuarantee;
        this.subscribeCommand = subscribeCommand;
        this.exceptionListenerSupplier = exceptionListenerSupplier;
        this.internalBufferSize = internalBufferSize;
        this.batchSize = batchSize;
        this.orderBy = orderBy;
        this.reconnectDelayStrategySupplier = reconnectDelayStrategySupplier;
        this.heartbeat = heartbeat;
        this.config = config;
        this.skipN = skipN;
        this.maxBacklog = maxBacklog;
        this.useSuffix = useSuffix;
        this.pruneInterval = pruneInterval;
        this.sleepMillisAfterBlock = sleepMillisAfterBlock;
        this.discardAfterEmit = discardAfterEmit;
        this.headerKeys = headerKeys;
        this.initCallback = initCallback;
    }

    /**
     * Returns the boundedness of this source.
     *
     * @return The boundedness of this source.
     */
    @Override
    public Boundedness getBoundedness() {
        if (topN > 0 || subscribeCommand.equals("sow") || allAreBookmarkRange()) {
            return Boundedness.BOUNDED;
        } else {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }
    }

    /**
     * Returns whether or not all the bookmarks use a bookmark range.
     *
     * @Return Whether or not all the bookmarks are a range.
     */
    protected boolean allAreBookmarkRange() {
        if (bookmark != null && !bookmark.endsWith(")") && !bookmark.endsWith("]")) return false;
        for (AMPSSplit split : splits) {
            if (split.getBookmark() != null && 
                    !split.getBookmark().endsWith(")") && 
                    !split.getBookmark().endsWith("]")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a new {@link AMPSSourceReader} to read data from the source.
     *
     * @param readerContext The context for the reader.
     * @return A new source reader.
     * @throws Exception if any error occurs during reader creation.
     */
    @Override
    public SourceReader<OUT, AMPSSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return createReaderWithClient(readerContext, new HAClient());
    }
    
    /**
     * Creates a new {@link AMPSSourceReader} using the {@link HAClient} provided by the parameters.
     *
     * @param readerContext The context for the reader.
     * @param client The HAClient that the reader will use.
     * @return A new source reader.
     * @throws Exception if any error occurs during reader creation.
     */
    @VisibleForTesting
    public SourceReader<OUT, AMPSSplit> createReaderWithClient(SourceReaderContext readerContext, HAClient client) throws Exception {
        return new AMPSSourceReader<>(
            readerContext, 
            deserializationSchema, 
            uri, 
            topic, 
            clientName, 
            contentFilter,
            options, 
            bookmarkStoreFunction, 
            serverChooserSupplier == null ? null : serverChooserSupplier.get(), 
            bookmark, 
            queueSemantics,
            ackBatchSize, 
            ackTimeout, 
            topN, 
            deliveryGuarantee,
            subscribeCommand,
            exceptionListenerSupplier == null ? null : exceptionListenerSupplier.get(),
            internalBufferSize,
            batchSize,
            orderBy,
            reconnectDelayStrategySupplier == null ? null : reconnectDelayStrategySupplier.get(),
            heartbeat,
            client,
            config,
            skipN,
            maxBacklog,
            useSuffix || (readerContext.currentParallelism() > 1 && splits.size() > 1),
            pruneInterval,
            sleepMillisAfterBlock,
            discardAfterEmit,
            headerKeys,
            initCallback);
    }

    /**
     * Creates a new {@link AMPSSplitEnumerator} to enumerate the splits of the source.
     *
     * @param enumContext The context for the enumerator.
     * @return A new split enumerator.
     * @throws Exception if any error occurs during enumerator creation.
     */
    @Override
    public SplitEnumerator<AMPSSplit, AMPSCheckpoint> createEnumerator(
            SplitEnumeratorContext<AMPSSplit> enumContext) throws Exception {
        return new AMPSSplitEnumerator(enumContext, splits);
    }

    /**
     * Restores an {@link AMPSSplitEnumerator} from a checkpoint.
     *
     * @param enumContext The context for the enumerator.
     * @param checkpoint  The checkpoint to restore from.
     * @return A restored split enumerator.
     * @throws Exception if any error occurs during enumerator restoration.
     */
    @Override
    public SplitEnumerator<AMPSSplit, AMPSCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<AMPSSplit> enumContext, AMPSCheckpoint checkpoint) throws Exception {
        return new AMPSSplitEnumerator(enumContext, checkpoint);
    }

    /**
     * Returns the serializer for the splits of this source.
     *
     * @return The split serializer.
     */
    @Override
    public SimpleVersionedSerializer<AMPSSplit> getSplitSerializer() {
        return new AMPSSplitSerializer();
    }

    /**
     * Returns the serializer for the checkpoints of this source.
     *
     * @return The checkpoint serializer.
     */
    @Override
    public SimpleVersionedSerializer<AMPSCheckpoint> getEnumeratorCheckpointSerializer() {
        return new AMPSCheckpointSerializer();
    }

    /**
     * Returns the {@link TypeInformation} for the type of elements produced by this source.
     *
     * @return The type information.
     */
    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    /**
     * Creates a new {@link AMPSSourceBuilder} to construct an {@link AMPSSource}.
     * 
     * <p>
     * The following are required to create a valid {@link AMPSSource}:
     * <ul>
     *  <li>A uri or {@link ServerChooserSupplier}</li>
     *  <li>A topic or {@link List<AMPSSplit>}</li>
     *  <li>A {@link DeserializationSchema} or an {@link AMPSDeserializationSchema}</li>
     * </ul>
     *
     * @param <OUT> The type of elements produced by the source.
     * @return A new AMPSSourceBuilder.
     */
    public static <OUT> AMPSSourceBuilder<OUT> builder() {
        return new AMPSSourceBuilder<>();
    }

    /**
     * A builder for creating {@link AMPSSource} instances.
     *
     * <p>
     * The following are required to create a valid {@link AMPSSource}:
     * <ul>
     *  <li>A uri or {@link ServerChooserSupplier}</li>
     *  <li>A topic or {@link List<AMPSSplit>}</li>
     *  <li>A {@link DeserializationSchema} or an {@link AMPSDeserializationSchema}</li>
     * </ul>
     *
     * @param <OUT> The type of elements produced by the source.
     */
    @PublicEvolving
    public static class AMPSSourceBuilder<OUT> {
        protected String uri;
        protected String topic;
        protected String clientName = "AMPSSource";
        protected String contentFilter;
        protected String options;
        protected AMPSDeserializationSchema<OUT> deserializationSchema;
        protected BookmarkStoreFunction bookmarkStoreFunction;
        protected ServerChooserSupplier serverChooserSupplier;
        protected String bookmark;
        protected List<AMPSSplit> splits = new ArrayList<>();
        protected List<String> splitsAsStrings = new ArrayList<>();
        protected AMPSQueueSemantics queueSemantics = AMPSQueueSemantics.NONE;
        protected int ackBatchSize = 1;
        protected int ackTimeout = 0;
        protected int topN = 0;
        protected DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
        protected String subscribeCommand = "subscribe";
        protected ExceptionListenerSupplier exceptionListenerSupplier;
        protected int internalBufferSize = 1000;
        protected int batchSize = 10;
        protected String orderBy;
        protected ReconnectDelayStrategySupplier reconnectDelayStrategySupplier;
        protected int heartbeat = 0;
        protected Configuration config = new Configuration();
        protected int skipN = 0;
        protected int maxBacklog = 1;
        protected int sleepMillisAfterBlock = 0;
        protected boolean discardAfterEmit = false;
        protected boolean useSuffix = false;
        protected long pruneInterval = 30_000L;
        protected Set<AMPSSourceHeaderKeys> headerKeys = new HashSet<>();
        protected ConnectorInitializer initCallback;

        /**
         * Sets the connection URI for the AMPS server (e.g., "tcp://localhost:9007/amps/json").
         *
         * @param uri The AMPS connection URI.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * Sets the AMPS topic.
         *
         * @param topic The name of the topic.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Sets the client name to use when connecting to AMPS.
         *
         * @param clientName The client name.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setClientName(String clientName) {
            this.clientName = clientName;
            return this;
        }

        /**
         * Sets the content filter used by all readers. 
         *
         * This filter is added to any filter that an {@link AMPSSplit} provides.
         *
         * @param contentFilter The content filter expression.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setContentFilter(String contentFilter) {
            this.contentFilter = contentFilter;
            return this;
        }

        /**
         * Sets additional options.
         *
         * @param options The command options.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setOptions(String options) {
            this.options = options;
            return this;
        }

        /**
         * Sets the {@link AMPSDeserializationSchema} for deserializing messages from AMPS to records of type OUT.
         *
         * @param deserializationSchema The {@link AMPSDeserializationSchema}.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setDeserializationSchema(AMPSDeserializationSchema<OUT> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }
        
        /**
         * Sets the {@link AMPSDeserializationSchema} for deserializing messages from AMPS to records of type OUT.
         *
         * This method takes a {@link DeserializationSchema} and creates a {@link AMPSDataOnlyDeserializationSchemaWrapper}
         * to deserialize messages from AMPS to the output type.
         *
         * @param deserializationSchema The {@link DeserializationSchema}.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
            this.deserializationSchema = new AMPSDataOnlyDeserializationSchemaWrapper<>(deserializationSchema);
            return this;
        }

        /**
         * Sets the {@link BookmarkStoreFunction} for the subscription, enabling durable subscriptions.
         *
         * @param bookmarkStoreFunction The {@link BookmarkStoreFunction}.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setBookmarkStoreFunction(BookmarkStoreFunction bookmarkStoreFunction) {
            this.bookmarkStoreFunction = bookmarkStoreFunction;
            return this;
        }

        /**
         * Sets the {@link ServerChooserSupplier} for the HAClient, enabling advanced HA strategies.
         *
         * @param serverChooserSupplier The {@link ServerChooserSupplier}.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setServerChooserSupplier(ServerChooserSupplier serverChooserSupplier) {
            this.serverChooserSupplier = serverChooserSupplier;
            return this;
        }

        /**
         * Sets the starting bookmark for the subscription.
         *
         * @param bookmark The starting bookmark.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setBookmark(String bookmark) {
            this.bookmark = bookmark;
            return this;
        }

        /**
         * Sets the content filters that will be used as splits.
         *
         * The filters should shard the topic similar to the following: ["/id MOD 2 = 0", "/id MOD 2 = 1"].
         * Should not be used alongside {@link #setAMPSSplits(Collection<AMPSSplit>)}.
         * 
         * @param splits A {@link Collection} of strings that will be used for the splits.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setSplits(Collection<String> splits) {
            this.splitsAsStrings.addAll(splits);
            return this;
        }

        /**
         * Sets the splits.
         *
         * Should not be used alongside {@link #setSplits(Collection<String>)}.
         * 
         * @param splits A {@link Collection} of {@link AMPSSplit} objects that will be used for the splits.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setAMPSSplits(Collection<AMPSSplit> splits) {
            this.splits.addAll(splits);
            return this;
        }
        /**
         * Sets the semantics for an AMPS queue.
         *
         * This is only relevant when subscribed to a message queue.
         * 
         * <p>
         * Supported semantics include:
         * <ul>
         *  <li>"at-least-once"</li>
         *  <li>"at-most-once"</li>
         * </ul>
         * 
         * @param queueSemantics A string for the queue semantics.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setQueueSemantics(String queueSemantics) {
            switch (queueSemantics) {
                case "at-least-once" -> this.queueSemantics = AMPSQueueSemantics.AT_LEAST_ONCE;
                case "at-most-once" -> this.queueSemantics = AMPSQueueSemantics.AT_MOST_ONCE;
                default -> throw new UnsupportedOperationException("AMPSSource does not support queue semantics '" + queueSemantics + "'");
            }

            return this;
        }

        /**
         * Sets the ack batch size for readers of this source.
         *
         * This is only relevant when subscribed to a message queue.
         * 
         * @param ackBatchSize The ack batch size of an individual reader in this source.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setAckBatchSize(int ackBatchSize) {
            this.ackBatchSize = ackBatchSize;
            return this;
        }

        /**
         * Sets the ack timeout for readers of this source.
         *
         * This is only relevant when subscribed to a message queue.
         * 
         * @param ackTimeout The ack timeout for an individual reader in this source.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setAckTimeout(int ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        /**
         * Sets the topN for readers of this source.
         *
         * This is only relevant for bookmark subscriptions and SOW queries.
         *
         * @param topN The amount of messages to read from AMPS.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setTopN(int topN) {
            this.topN = topN;
            return this;
        }

        /**
         * Sets the {@link DeliveryGuarantee} for the source.
         * 
         * <p>
         * When set to DeliveryGuarantee.AT_LEAST_ONCE or DeliveryGuarantee.EXACTLY_ONCE,
         * a {@link com.crankuptheamps.client.BookmarkStore} is required.
         * By default, a {@link com.crankuptheamps.client.MemoryBookmarkStore} is used along with a
         * {@link com.crankuptheamps.client.RecoveryPointAdapter}.
         *
         * <p>
         * There is currently no difference between DeliveryGuarantee.AT_LEAST_ONCE and DeliveryGuarantee.EXACTLY_ONCE.
         * Both provide the same delivery guarantee of at least once.
         *
         * @param deliveryGuarantee The delivery guarantee.          
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        /**
         * Sets the subscribe command. 
         *
         * Default is "subscribe".
         *
         * <p>
         * Supported commands include:
         * <ul>
         *  <li>"subscribe"</li>
         *  <li>"sow"</li>
         *  <li>"sow_and_subscribe"</li>
         *  <li>"delta_subscribe"</li>
         *  <li>"sow_and_delta_subscribe"</li>
         * </ul>
         *
         * <p>
         * The commands "delta_subscribe" and "sow_and_delta_subscribe" may require a custom 
         * {@link DeserializationSchema} to ensure all fields are properly set.
         *
         * @param subscribeCommand The command for subscribing to AMPS.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setSubscribeCommand(String subscribeCommand) {
            if (subscribeCommand.equals("subscribe") ||
                    subscribeCommand.equals("sow") || 
                    subscribeCommand.equals("sow_and_subscribe") ||
                    subscribeCommand.equals("delta_subscribe") ||
                    subscribeCommand.equals("sow_and_delta_subscribe")) {
                this.subscribeCommand = subscribeCommand;
            } else {
                throw new UnsupportedOperationException("AMPSSource does not support command '" + subscribeCommand + "'");
            }

            return this;
        }

        /**
         * Sets the {@link ExceptionListenerSupplier} for the subscription, allowing exceptions that are absorbed by the AMPS client to be communicated.
         *
         * @param exceptionListenerSupplier The {@link ExceptionListenerSupplier}.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setExceptionListenerSupplier(ExceptionListenerSupplier exceptionListenerSupplier) {
            this.exceptionListenerSupplier = exceptionListenerSupplier;
            return this;
        }

        /**
         * Sets the buffer size for the queue that each client uses to buffer messages from AMPS.
         *
         * Default size is 1000.
         * Higher values may increase performance while also increasing memory use.
         *
         * @param internalBufferSize The size of the internal buffer.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setInternalBufferSize(int internalBufferSize) {
            this.internalBufferSize = internalBufferSize;
            return this;
        }

        /**
         * Sets the batch size for readers of this source. This is only relevant for SOW queries.
         * 
         * @param batchSize The batch size of an individual reader in this source.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        /**
         * Sets how the SOW results should be ordered.
         *
         * @param orderBy The String for how the SOW results should be ordered.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setOrderBy(String orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        /**
         * Sets the {@link ReconnectDelayStrategySupplier} for the clients.
         *
         * @param reconnectDelayStrategySupplier The {@link ReconnectDelayStrategySupplier}.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setReconnectDelayStrategySupplier(ReconnectDelayStrategySupplier reconnectDelayStrategySupplier) {
            this.reconnectDelayStrategySupplier = reconnectDelayStrategySupplier;
            return this;
        }

        /**
         * Sets the heartbeat for the client.
         *
         * @param intervalSeconds The seconds between beats from the server.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setHeartbeat(int intervalSeconds) {
            this.heartbeat = intervalSeconds;
            return this;
        }
        
        /**
         * Sets the {@link Configuration} for the client.
         *
         * The configuration will be used to supply {@link org.apache.flink.connector.base.source.reader.SourceReaderOptions} to the reader.
         *
         * @param config The Configuration.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setConfiguration(Configuration config) {
            this.config = config;
            return this;
        }
        
        /**
         * Sets the skipN for readers of this source. 
         *
         * This is only relevant for SOW queries.
         * This is equivalent to setting an option with "skip_n=n".
         * Should not be used alongside {@link #setOptions(String)} with the option "skip_n=n".
         *
         * @param skipN The amount of messages to skip from AMPS.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setSkipN(int skipN) {
            this.skipN = skipN;
            return this;
        }

        /**
         * Sets the max backlog for readers of this source.
         *
         * This is only relevant when subscribed to a message queue.
         * This is equivalent to setting an option with "max_backlog=n".
         * Should not be used alongside {@link #setOptions(String)} with the option "max_backlog=n".
         *
         * @param maxBacklog The max backlog for messages from a queue.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setMaxBacklog(int maxBacklog) {
            this.maxBacklog = maxBacklog;
            return this;
        }

        /**
         * Sets the amount of milliseconds to sleep for after blocking for messages.
         * 
         * Default value is 0.
         *
         * <p>
         * When the {@link AMPSSourceReader} fetches messages, it will block if there are no messages and immediately wake up once a message arrives. 
         * This method can be used to allow some messages to buffer in the queue by making it sleep for
         * a short duration before taking the messages from the queue, which can increase throughput at the cost of latency.
         * 
         * @param sleepMillisAfterBlock The milliseconds to sleep for after blocking for messages.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setSleepMillisAfterBlock(int sleepMillisAfterBlock) {
            this.sleepMillisAfterBlock = sleepMillisAfterBlock;
            return this;
        }

        /**
         * Sets the flag for if bookmarks should be discarded after a record is emitted rather than
         * on checkpoint completion.
         *
         * Default value is false.
         *
         * <p>
         * When false, a {@link com.crankuptheamps.client.BookmarkStore} can only be used when checkpointing is enabled as bookmarks
         * are discarded on checkpoint completion. This will increase throughput at the cost of memory use as many bookmarks will
         * be stored until checkpoint completion, which is when they will be discarded.
         *
         * <p>
         * When true, a {@link com.crankuptheamps.client.BookmarkStore} can be used regardless of checkpointing as bookmarks are
         * discarded when records are emitted. This will decrease memory use at the cost of throughput as fewer bookmarks will
         * be stored at any given time, but every record will need its bookmark discarded before the next record can be emitted.
         *
         * @param discardAfterEmit Flag for if bookmarks should be discarded after a record is emitted.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setDiscardAfterEmit(boolean discardAfterEmit) {
            this.discardAfterEmit = discardAfterEmit;
            return this;
        }
        
        /**
         * Sets the flag for if a suffix should be appended to the client name.
         *
         * Default value is false, but if parallelism is greater than 1, it will always be true.
         *
         * <p>
         * The suffix is used to ensure unique client names among readers. Each client needs a unique client name,
         * so this field ensures that each client receives a unique client name.
         *
         * <p>
         * The suffix is the same for an individual job submission even in the event of failures, but it will
         * be different across several submissions. For example, if job A is submitted and finishes, submitting
         * job A again will result in different suffixes compared to the first submission.
         *
         * @param useSuffix Flag for if a suffix is appened to the client name.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setUseSuffix(boolean useSuffix) {
            this.useSuffix = useSuffix;
            return this;
        }

        /**
         * Sets the prune interval if the {@link com.crankuptheamps.client.BookmarkStore} is a
         * {@link com.crankuptheamps.client.LoggedBookmarkStore}.
         *
         * Default value is 30_000L or 30 seconds.
         *
         * @param pruneInterval The amount of milliseconds that must pass before the bookmark store is pruned.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setPruneInterval(long pruneInterval) {
            this.pruneInterval = pruneInterval;
            return this;
        }

        /**
         * Sets the headers that should be preserved from a {@link com.crankuptheamps.client.Message}.
         *
         * The valid headers are specified in {@link AMPSSourceHeaderKeys}.
         *
         * @param headerKeys The {@link Collection} of {@link AMPSSourceHeaderKeys} to preserve.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setHeaderKeys(Collection<AMPSSourceHeaderKeys> headerKeys) {
            this.headerKeys = Set.copyOf(headerKeys);
            return this;
        }

        /**
         * Sets the functional interface that will be called before the {@link HAClient} connects
         * to AMPS.
         *
         * @param initCallback The {@link ConnectorInitializer} that will be called.
         * @return This builder.
         */
        public AMPSSourceBuilder<OUT> setConnectorInitializer(ConnectorInitializer initCallback) {
            this.initCallback = initCallback;
            return this;
        }

        /**
         * Builds the {@link AMPSSource} with the configured properties.
         *
         * @return A new {@link AMPSSource} instance.
         */
        public AMPSSource<OUT> build() {
            // Convert the List<String> of splits into a List<AMPSSplit> using the topic
            if (!splitsAsStrings.isEmpty() && splits.isEmpty()) {
                splitsAsStrings.forEach(split -> splits.add(new AMPSSplit(topic, split)));
            }
            
            // Create a split if no splits are given
            if (splits.isEmpty()) {
                splits.add(new AMPSSplit(topic, "", bookmark == null ? "" : bookmark));
            }

            return new AMPSSource<>(uri, topic, clientName, contentFilter, options, deserializationSchema,
                                    bookmarkStoreFunction, serverChooserSupplier, bookmark, splits, queueSemantics,
                                    ackBatchSize, ackTimeout, topN, deliveryGuarantee, subscribeCommand, exceptionListenerSupplier,
                                    internalBufferSize, batchSize, orderBy, reconnectDelayStrategySupplier, heartbeat,
                                    config, skipN, maxBacklog, useSuffix, pruneInterval, sleepMillisAfterBlock, discardAfterEmit,
                                    headerKeys, initCallback);
        }
    }
}

