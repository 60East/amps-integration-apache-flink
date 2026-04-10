# Apache Flink AMPS Connector

The Apache Flink AMPS connector provides AMPS clients to Apache Flink for publishing and consuming data.

## Introduction

The connector allows developers to focus on business logic by reducing the cognitive and operational overhead of managing HA AMPS clients.
A FLIP-27 source and a SinkV2 sink are provided for communication between Apache Flink and AMPS.
The connectors are located in the `flink-connector-amps/` directory.
Examples using the connectors are located in `flink-connector-amps-examples/` directory.

## AMPSSource

The AMPSSource is the FLIP-27 source for consuming data from AMPS. An AMPSSource can be constructed using the `builder()` method. Some methods are required to construct a valid AMPSSource while others are for specific functionality.

### Required

The following are required to construct a valid AMPSSource:

- `.setUri(String)` or `.setServerChooserSupplier(Supplier<ServerChooser>)`
    - Sets the URI or **ServerChooser** that will be used to connect to AMPS.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setTopic(String)` or `.setAMPSSplits(Collection<AMPSSplit>)`
    - Sets the topic that the clients will subscribe to or an **AMPSSplit** **Collection** that clients will use to create subscriptions.

- `.setDeserializationSchema(DeserializationSchema)` or `.setDeserializationSchema(AMPSDeserializationSchema)`
    - Sets the deserializer that clients will use to deserialize data from AMPS.
    - When using a **DeserializationSchema**, only the data part of a message from AMPS will be used to emit a record to Flink.
    - When using an **AMPSDeserializationSchema**, more control over message deserialization is provided.

- `.build()`
    - Returns the constructed AMPSSource.

### Optional - AMPS Client

The following are optional for functionality related to the AMPS client:

- `.setClientName(String)`
    - Sets the base client name.

- `.setContentFilter(String)`
    - Sets the content filter used by all clients from this source.
    - This can be used to provide a filter that all clients should use in addition to any filter provided by a split.

- `.setOptions(String)`
    - Sets additional options.

- `.setBookmarkStoreFunction(SerializableFunction<String, BookmarkStore>)`
    - Sets the **SerializableFunction** (com.crankuptheamps.client.util.SerializableFunction) that gets each client a **BookmarkStore**. 
    - The parameter is the client name that the reader's client will use. This client name will be unique in the job that uses the source, so it can be used in the path for a **LoggedBookmarkStore**.
    - **Note**: By default, the AMPSSource uses a **MemoryBookmarkStore** and **RecoveryPointAdapter** to work with Flink's checkpointing when the source's delivery guarantee is set to AT_LEAST_ONCE or EXACTLY_ONCE. If custom logic is required, then this method can be used to use the supplied **BookmarkStore**.
    - **Note**: This should only be used when either checkpointing is enabled or discardAfterEmit is true.

- `.setBookmark(String)`
    - Sets the starting bookmark.
    - Only valid for topics with a transaction log.

- `.setSplits(Collection<String>)`
    - Sets the splits for the AMPSSource using a **Collection** of content filters to split on.
    - This can be used instead of `.setAMPSSplits(Collection<AMPSSplit>)` to use content filters on the topic set by `.setTopic(String)`. Refer to the [examples](#ampssource-examples) for proper usage.

- `.setQueueSemantics(String)`
    - Sets the queue semantics if the AMPSSource is subscribing to a queue.
    - Only valid for queues.
    - Valid semantics are:
        - "at-least-once"
        - "at-most-once"

- `.setAckBatchSize(int)`
    - Sets how many acknowledgement messages will be batched.
    - Only valid for queues.

- `.setAckTimeout(int)`
    - Sets how long acknowledgement messages will be held.
    - Only valid for queues.

- `.setTopN(int)`
    - Sets the amount of messages that should be received from AMPS. 
    - Only valid for SOW queries and topics with a transaction log.
    - Makes the source **BOUNDED** rather than **CONTINUOUS_UNBOUNDED**. 

- `.setSkipN(int)`
    - Sets the amount of messages in a SOW query that should be skipped.
    - Only valid for SOW queries with a set topN.
    - **Note**: If "skip_n=n" is set using options, then this value will be ignored.

- `.setSubscribeCommand(String)`
    - Sets the subscription command for the source.
    - Valid commands include:
        - "subscribe" (Default)
        - "sow" (Makes the source **BOUNDED**)
        - "sow_and_subscribe"
    - The following commands are valid but may require custom deserialization schemas:
        - "delta_subscribe"
        - "sow_and_delta_subscribe"

- `.setExceptionListenerSupplier(Supplier<ExceptionListener>)`
    - Sets the **ExceptionListener** that clients will use.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setBatchSize(int)`
    - Sets the batch size for SOW queries.
    - Only valid for SOW queries.

- `.setOrderBy(String)`
    - Sets how SOW results should be ordered.
    - Only valid for SOW queries.

- `.setReconnectDelayStrategySupplier(Supplier<ReconnectDelayStrategy>)`
    - Sets the **ReconnectDelayStrategy** that clients will use.
    - Default uses exponential delay.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setHeartbeat(int)`
    - Sets the heartbeat interval in seconds for the clients.

- `.setMaxBacklog(int)`
    - Sets the max backlog when subscribing to a message queue.
    - Only valid for queues.
    - **Note**: If "max_backlog" is set using options, then this value will be ignored.

- `.setPruneInterval(long)`
    - Sets the milliseconds that must pass before a **LoggedBookmarkStore** is pruned.
    - Default is 30_000L or 30 seconds.
    - This field only has an effect if there is a **BookmarkStore** defined and it is a **LoggedBookmarkStore**.

- `.setHeaderKeys(Collection<AMPSSourceHeaderKeys>)`
    - Sets the headers that should be preserved from a message from AMPS.
    - The headers will be preserved in an **AMPSMessage** and can only be accessed using the `.getHeader(AMPSSourceHeaderKeys)` method.
    - **Note**: The headers are only used in a user defined **AMPSDeserializationSchema** provided to the source by the `.setDeserializationSchema(AMPSDeserializationSchema)` method.

- `.setConnectorInitializer(ConnectorInitializer)`
    - Sets the **ConnectorInitializer** that will run its init(HAClient) method before the reader connects to AMPS.
    - This can be used to execute code such as setting up an **SSLContext** for the AMPS clients.

### Optional - Other

The following are optional for additional functionality not directly related to the AMPS client:

- `.setDeliveryGuarantee(DeliveryGuarantee)`
    - Sets the source's delivery guarantee by setting up a **BookmarkStore**.
    - Default is NONE.
    - When set to AT_LEAST_ONCE or EXACTLY_ONCE, a **MemoryBookmarkStore** with a **RecoveryPointAdapter** will be used by default to achieve the given delivery guarantee. In other words, using a delivery guarantee other than NONE will set up a **BookmarkStore** without needing to create a **BookmarkStoreFunction**. If set to AT_LEAST_ONCE or EXACTLY_ONCE when a **BookmarkStoreFunction** is also given, the **BookmarkStore** provided by the **BookmarkStoreFunction** will be used over the default **MemoryBookmarkStore**.
    - **Note**: AT_LEAST_ONCE and EXACTLY_ONCE currently behave the same.
    - **Note**: This should be set to NONE if both checkpointing is not enabled and discardAfterEmit is set to false.

- `.setInternalBufferSize(int)`
    - Sets the buffer size of the queue that each client uses to buffer messages from AMPS.
    - Default is 1000.
    - Larger sizes allow more messages to be buffered, so more messages can be given to Flink from the client at one time. However, it will increase memory use.

- `.setConfiguration(Configuration)`
    - Sets the Flink **Configuration** used to supply **SourceReaderOptions**.

- `.setSleepMillisAfterBlock(int)`
    - Sets the amount of milliseconds to sleep for after blocking for a message.
    - Default is 0.
    - When messages are being fetched from an AMPSSource's readers, they will block if there are no messages and wake up immediately once a message arrives. This method can be used to allow some messages to buffer in the queue by making a reader sleep for a short duration before getting the messages from the queue. In other words, setting this value can increase throughput at the cost of latency.

- `.setDiscardAfterEmit(boolean)`
    - Sets the flag for if bookmarks should be discarded after a record is emitted rather than on checkpoint completion.
    - Default is false.
    - The difference in performance is most clear when used in a job with a parallelism of 1.
    - When false, a **BookmarkStore** can only be used when checkpointing is enabled as bookmarks are discarded on checkpoint completion. This will increase throughput at the cost of memory use as many bookmarks will be stored until checkpoint completion, which is when they will be discarded.
    - When true, a **BookmarkStore** can be used regardless of checkpointing as bookmarks are discarded when records are emitted. This will decrease memory use at the cost of throughput as fewer bookmarks will be stored at any given time, but every record will need its bookmark discarded before the next record can be emitted.

- `.setUseSuffix(boolean)`
    - Sets the flag for if a suffix should be appended to the client name.
    - Default is false, but it will be set to true if parallelism is greater than 1 and there are multiple splits.
    - Client names need to be unique, so this field adds a suffix to the given client name to help maintain unique client names.

### AMPSSource Examples

The following are basic examples of how an AMPSSource can be constructed.

<br>

Simple AMPSSource that subscribes to messages:
```java
AMPSSource<String> source = AMPSSource.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setDeserializationSchema(new SimpleStringSchema())
    .build();
```

<br>

AMPSSource with a bookmark subscription that uses Flink's checkpointing to persist across Flink restarts:
```java
AMPSSource<String> source = AMPSSource.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setDeserializationSchema(new SimpleStringSchema())
    .setBookmark("0")
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // Automatically sets up a BookmarkStore that will work with Flink's checkpointing
    .build();
```

<br>

AMPSSource that makes a SOW query for a **BOUNDED** job:
```java
AMPSSource<String> source = AMPSSource.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setDeserializationSchema(new SimpleStringSchema())
    .setSubscribeCommand("sow")
    .build();
```

<br>

AMPSSource that uses two splits with content filters with one topic:
```java
List<String> splits = new ArrayList<>();
splits.add("/num MOD 2 = 0");
splits.add("/num MOD 2 = 1");

AMPSSource<String> source = AMPSSource.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setDeserializationSchema(new SimpleStringSchema())
    .setSplits(splits)
    .build();
```

<br>

AMPSSource that uses two splits with two topics:
```java
List<AMPSSplit> splits = new ArrayList<>();
splits.add(new AMPSSplit(topic1, ""));
splits.add(new AMPSSplit(topic2, ""));

AMPSSource<String> source = AMPSSource.<String>builder()
    .setUri(uri)
    .setDeserializationSchema(new SimpleStringSchema())
    .setAMPSSplits(splits)
    .build();
```

<br>

AMPSSource that reads from a queue:
```java
AMPSSource<String> source = AMPSSource.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setDeserializationSchema(new SimpleStringSchema())
    .setQueueSemantics("at-least-once")
    .setMaxBacklog(20)      // Optional and can also be set with .setOptions("max_backlog=20")
    .setAckBatchSize(5)     // Optional
    .setAckTimeout(5000)    // Optional
    .build();
```

## AMPSSink

The AMPSSink is the SinkV2 sink for publishing data to AMPS. An AMPSSink can be constructed using the `builder()` method. Some methods are required to construct a valid AMPSSink while others are for specific functionality.

### Required

The following are required to construct a valid AMPSSink:

- `.setUri(String)` or `.setServerChooserSupplier(Supplier<ServerChooser>)`
    - Sets the URI or **ServerChooser** that will be used to connect to AMPS.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setTopic(String)`
    - Sets the topic that the clients will publish to.

- `.setSerializationSchema(SerializationSchema)` or `.setSerializationSchema(AMPSSerializationSchema)`
    - Sets the serializer that clients will use to serialize data to AMPS.
    - When using a **SerializationSchema**, only the schema will be used to serialize the input elements.
    - When using an **AMPSSerializationSchema**, more control over element serialization is provided such as specifying a correlation ID for each message published to AMPS.

- `.build()`
    - Returns the constructed AMPSSink.

### Optional - AMPS Client

The following are optional for functionality related to the AMPS client:

- `.setClientName(String)`
    - Sets the client name.

- `.setPublishStoreFunction(SerializableFunction<Store>)`
    - Sets the **SerializableFunction** (com.crankuptheamps.client.util.SerializableFunction) that gets each client a **Store**.
    - The parameter is the client name that the writer's client will use. This client name will be unique in the job that uses the sink, so it can be used in the path for a **PublishStore**.

- `.setPublishCommand(String)`
    - Sets the publish command for the sink.
    - Valid commands include:
        - "publish" (Default)
        - "sow_delete" (Performs a SOW delete by data)
    - The following commands are valid but may require custom serialization schemas:
        - "delta_publish"

- `.setFailedWriteHandlerSupplier(Supplier<FailedWriteHandler>)`
    - Sets the supplier that gets each client a **FailedWriteHandler**.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setReconnectDelayStrategySupplier(Supplier<ReconnectDelayStrategy>)`
    - Sets the **ReconnectDelayStrategy** that clients will use.
    - Default uses exponential delay.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setExceptionListenerSupplier(Supplier<ExceptionListener>)`
    - Sets the **ExceptionListener** that clients will use.
    - **Note**: The **Supplier** must be **Serializable**.

- `.setHeartbeat(int)`
    - Sets the heartbeat interval in seconds for the clients.

- `.setRetryOnDisconnect(boolean)`
    - Set whether or not messages being sent to the server should retry if the client is disconnected.

- `.setExpiration(int)`
    - Sets the expiration for SOW/queue messages sent.

- `.setFlushTimeout(long)`
    - Sets the amount of milliseconds clients will wait for a flush.
    - Default is 10_000L or 10 seconds.

- `.setCorrelationId(String)`
    - Sets the correlation ID to use on every message sent to AMPS from this sink.
    - Must contain only base64 characters.
    - **Note**: The correlation ID provided by an **AMPSSerializationSchema** will override the correlation ID provided here. If there is no correlation ID provided by the schema, then this value will be used.

- `.setConnectorInitializer(ConnectorInitializer)`
    - Sets the **ConnectorInitializer** that will run its init(HAClient) method before the reader connects to AMPS.
    - This can be used to execute code such as setting up an **SSLContext** for the AMPS Clients.

### Optional - Other

The following are optional for additional functionality not directly related to the AMPS client:

- `.setDeliveryGuarantee(DeliveryGuarantee)`
    - Sets the delivery guarantee for the sink.
    - Default is NONE.
    - **Note**: For AT_LEAST_ONCE and EXACTLY_ONCE, checkpointing and a **PublishStore** is required. A **MemoryPublishStore** is used by default.
    - **Note**: For EXACTLY_ONCE, timestamps must be provided. These timestamps must be strictly increasing. When using an AMPSSource, this can be done by including `Message.Options.Timestamp` or "timestamp" in the options.
    - **Note**: This should be set to NONE if checkpointing is not enabled.
    - **Note**: If the sink is receiving messages from a message queue, the sink should use AT_LEAST_ONCE rather than EXACTLY_ONCE.

- `.setUseSuffix(boolean)`
    - Sets the flag for if a suffix should be appended to the client name.
    - Default is false, but it will be set to true if parallelism is greater than 1.
    - Client names need to be unique, so this field adds a suffix to the given client name to help maintain unique client names.

### AMPSSink Examples

The following are basic examples of how an AMPSSink can be constructed.

<br>

AMPSSink that publishes messages:
```java
AMPSSink<String> sink = AMPSSink.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setSerializationSchema(new SimpleStringSchema())
    .build();
```

<br>

AMPSSink with a **Store**:
```java
AMPSSink<String> sink = AMPSSink.<String>builder()
    .setUri(uri)
    .setTopic(topic)
    .setSerializationSchema(new SimpleStringSchema())
    .setPublishStoreFunction((clientName) -> {
        try {
            return new MemoryPublishStore(100);
        } catch (StoreException se) {
            throw new RuntimeException(se);
        }
    })
    .build();
```

