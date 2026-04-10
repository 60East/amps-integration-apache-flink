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

package com.crankuptheamps.flink.example;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.ServerChooser;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;
import com.crankuptheamps.flink.util.function.ServerChooserSupplier;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.connector.dsv2.Source;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

/** Flink job that showcases replication using two AMPS instances. */
public class ReplicationExample {
    private static final String AMPS_1_URI = Constants.URI.JSON_1;
    private static final String AMPS_2_URI = Constants.URI.JSON_2;
    private static final String TXLOG_TOPIC_INITIAL = "initial-publish";
    private static final String TXLOG_TOPIC_MODIFIED = "with-ts";
    private static final String TXLOG_TOPIC_CHECKPOINT_FAILOVER = "checkpoint-failover";
    private static final String Q_WORK = "work";
    private static final String Q_WORK_TO_DO = "work-to-do";
    private static final int PARALLELISM = 1;

    // For Replication and Queue
    public static final String DATA = "example data";
    private static final long PUBLISH_AMOUNT = 10000;
    private static final int PUBLISH_RATE = 1;
    private static final boolean PRINT_TO_CONSOLE = true;
    
    // For Checkpoint failover
    private static final int MESSAGE_TO_FAIL_AT = 500;
    private static final int MESSAGES_PER_PRINT = 1;
    private static final int CHECKPOINTING_INTERVAL = 2000;
    private static final String OPTIONS = "rate=10";

    // For Queue
    private static final String BACKLOG = "max_backlog=2000";
    private static final int ACK_BATCH_SIZE = 1000;
    private static final int ACK_TIMEOUT = 10000;

    /*
     * These examples involve replication among the AMPS instances using the amps1 and amps2 configs.
     * Some of the examples also support demonstrating failover.
     */
    public static void main(String[] args) throws Exception {
        /*
         * Shows the connectors utilizing replication.
         * Does not support failover testing.
         *
         * This job involves initial messages being published to amps1,
         * those messages being replicated to amps2, a source and sink
         * subscribing and publishing a modified message to amps2, that
         * modified message being replicated to amps1, and finally,
         * a source subscribed to amps1 to publish the replicated modified
         * messages to an adhoc topic.
         */
        testReplication();

        /* Shows the connectors utilizing replication with message queues.
         * The amps2 instance can be disabled to show the queue subscriber source flipping to amps1.
         *
         * This job involves queue messages being published to amps1, which are replicated to amps2.
         * A source connected to amps2 processes messages from the queue, which are sinked to amps1
         * where a client publishes the messages to the console.
         */
        //testQueueReplication();

        /* Shows the connectors utilizing checkpointing and replication.
         * Fully supports failover testing by disabling either amps1 or amps2 repeatedly.
         *
         * This job involves a source that subscribes to either amps1 or amps2 and resumes its subscription
         * using its bookmark store. It also recovers on failure. It has a sink that publishes to amps to
         * show that no messages are lost.
         */
        //testCheckpointFailover();
    }

    private static void testCheckpointFailover() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        ((ExecutionEnvironmentImpl) env).setParallelism(1);
        
        CheckpointConfig cc = ((ExecutionEnvironmentImpl) env).getCheckpointCfg();
        cc.setCheckpointInterval(CHECKPOINTING_INTERVAL);
        cc.setTolerableCheckpointFailureNumber(100);

        Configuration c = ((ExecutionEnvironmentImpl) env).getConfiguration();
        c.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));

        // Generate messages and publish to AMPS_1

        Source<ReplicationMessage> messageGen = DataStreamV2SourceUtils.wrapSource(
            new ReplicationMessageGenerator(
                10000,
                RateLimiterStrategy.noOp()));
        
        Sink<ReplicationMessage> messageGenSink = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<ReplicationMessage>builder()
                .setUri(AMPS_1_URI)
                .setTopic(TXLOG_TOPIC_CHECKPOINT_FAILOVER)
                .setClientName("message-generator-sink")
                .setSerializationSchema(new JsonSerializationSchema<>())
                .build());

        env.fromSource(messageGen, "Message Generator").toSink(messageGenSink);
        
        String[] uris = {AMPS_1_URI, AMPS_2_URI};

        Source<ReplicationMessage> src = DataStreamV2SourceUtils.wrapSource(
            AMPSSource.<ReplicationMessage>builder()
                .setBookmark("0")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTopic(TXLOG_TOPIC_CHECKPOINT_FAILOVER)
                .setClientName("message-checkpointing-source")
                .setDeserializationSchema(new JsonDeserializationSchema<>(ReplicationMessage.class))
                .setOptions(OPTIONS)
                .setServerChooserSupplier(
                    new ReplicationServerChooserSupplier(uris))
                .build());

        Sink<String> snk = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<String>builder()
                .setUri(Constants.URI.JSON)
                .setTopic(Constants.TOPIC.ADHOC_MESSAGE)
                .setClientName("message-checkpointing-sink")
                .setSerializationSchema(new SimpleStringSchema())
                .build());

        env.fromSource(src, "V2 Checkpointing with Failover")
            .process(new OneInputStreamProcessFunction<ReplicationMessage, String>() {
                private int counter = 1;
                private int failCounter = 1;

                @Override
                public void processRecord(ReplicationMessage record, org.apache.flink.datastream.api.common.Collector<String> output, PartitionedContext<String> ctx) {
                    if (failCounter >= MESSAGE_TO_FAIL_AT) {
                        output.collect(record.toString() + " (Fail)");
                        throw new RuntimeException("Intentional Exception");
                    }

                    if (counter >= MESSAGES_PER_PRINT) {
                        output.collect(record.toString());
                    }

                    counter++;
                    failCounter++;
                }
            })
            .toSink(snk);

        Thread t = new Thread(new SubscriberRunnable(
            Constants.URI.JSON,
            Constants.TOPIC.ADHOC_MESSAGE,
            "",
            "",
            "",
            "subscribe"
        ));
        t.start();
        
        try {
            env.execute("V2 Checkpointing Failover Test");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            t.interrupt();
        }
    }

    private static void testReplication() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        ((ExecutionEnvironmentImpl) env).setParallelism(PARALLELISM);
        
        // Generate messages and publish to AMPS_1

        Source<ReplicationMessage> messageGen = DataStreamV2SourceUtils.wrapSource(
            new ReplicationMessageGenerator(
                PUBLISH_AMOUNT,
                RateLimiterStrategy.perSecond(PUBLISH_RATE)));
        
        Sink<ReplicationMessage> messageGenSink = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<ReplicationMessage>builder()
                .setUri(AMPS_1_URI)
                .setTopic(TXLOG_TOPIC_INITIAL)
                .setClientName("message-generator-sink")
                .setSerializationSchema(new JsonSerializationSchema<>())
                .build());

        env.fromSource(messageGen, "Message Generator").toSink(messageGenSink);

        // Subscribe to messages from AMPS_2 and publish to AMPS_2 the modified message
       
        Source<ReplicationMessage> modifySource = DataStreamV2SourceUtils.wrapSource(
            AMPSSource.<ReplicationMessage>builder()
                .setUri(AMPS_2_URI)
                .setTopic(TXLOG_TOPIC_INITIAL)
                .setClientName("message-modifier-source")
                .setDeserializationSchema(new JsonDeserializationSchema<>(ReplicationMessage.class))
                .build());

        Sink<ModifiedMessage> modifySink = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<ModifiedMessage>builder()
                .setUri(AMPS_2_URI)
                .setTopic(TXLOG_TOPIC_MODIFIED)
                .setClientName("message-modifier-sink")
                .setSerializationSchema(new JsonSerializationSchema<>())
                .build());

        env.fromSource(modifySource, "Message Modifier Source")
            .process(new OneInputStreamProcessFunction<ReplicationMessage, ModifiedMessage>() {
                @Override
                public void processRecord(ReplicationMessage record, Collector<ModifiedMessage> output, PartitionedContext<ModifiedMessage> ctx) {
                    ModifiedMessage m = new ModifiedMessage();

                    m.counter = record.counter;
                    m.group = record.group;
                    m.ts = Timestamp.from(Instant.now());
                    m.data = record.data;

                    output.collect(m);
                }
            })
            .toSink(modifySink);

        // Subscribe to modified messages from AMPS_1 and publish an adhoc message
        
        Source<ModifiedMessage> strSource = DataStreamV2SourceUtils.wrapSource(
            AMPSSource.<ModifiedMessage>builder()
                .setUri(AMPS_1_URI)
                .setTopic(TXLOG_TOPIC_MODIFIED)
                .setClientName("message-to-string-source")
                .setDeserializationSchema(new JsonDeserializationSchema<>(ModifiedMessage.class))
                .build());

        Sink<String> strSink = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<String>builder()
                .setUri(AMPS_1_URI)
                .setTopic("messages")
                .setClientName("message-to-string-sink")
                .setSerializationSchema(new SimpleStringSchema())
                .build());

        env.fromSource(strSource, "Message String Source")
            .process(new OneInputStreamProcessFunction<ModifiedMessage, String>() {
                @Override
                public void processRecord(ModifiedMessage record, Collector<String> output, PartitionedContext<String> ctx) {
                    output.collect(record.toString());
                }
            })
            .toSink(strSink);

        Thread originalMsgSub = new Thread(new SubscriberRunnable(
            AMPS_1_URI,
            TXLOG_TOPIC_INITIAL,
            "",
            "",
            "",
            "subscribe"
        ));
        Thread modifiedMsgSub = new Thread(new SubscriberRunnable(
            AMPS_1_URI,
            "messages",
            "",
            "",
            "",
            "subscribe"
        ));
        if (PRINT_TO_CONSOLE) {
            originalMsgSub.start();
            modifiedMsgSub.start();
        }
 
        try {
            env.execute("Replication Example"); 
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            originalMsgSub.interrupt();
            modifiedMsgSub.interrupt();
        }
    }
    
    private static void testQueueReplication() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        ((ExecutionEnvironmentImpl) env).setParallelism(PARALLELISM);
        
        // Generate messages and publish to AMPS_1's queue

        Source<ReplicationMessage> messageGen = DataStreamV2SourceUtils.wrapSource(
            new ReplicationMessageGenerator(
                PUBLISH_AMOUNT,
                RateLimiterStrategy.perSecond(PUBLISH_RATE)));
        
        Sink<ReplicationMessage> messageGenSink = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<ReplicationMessage>builder()
                .setUri(AMPS_1_URI)
                .setTopic(Q_WORK)
                .setClientName("queue-generator-sink")
                .setSerializationSchema(new JsonSerializationSchema<>())
                .build());

        env.fromSource(messageGen, "Queue Message Generator").toSink(messageGenSink);

        // Subscribe AMPS_2's queue (source can flip to AMPS_1)
       
        String[] uris = {AMPS_2_URI, AMPS_1_URI};

        Source<ReplicationMessage> queueConsumer = DataStreamV2SourceUtils.wrapSource(
            AMPSSource.<ReplicationMessage>builder()
                .setTopic(Q_WORK_TO_DO)
                .setClientName("queue-consumer")
                .setQueueSemantics("at-most-once")
                .setAckBatchSize(ACK_BATCH_SIZE)
                .setAckTimeout(ACK_TIMEOUT)
                .setOptions(BACKLOG)
                .setDeserializationSchema(new JsonDeserializationSchema<>(ReplicationMessage.class))
                .setServerChooserSupplier(
                    new ReplicationServerChooserSupplier(uris))
                .build());

        Sink<String> strSink = DataStreamV2SinkUtils.wrapSink(
            AMPSSink.<String>builder()
                .setUri(AMPS_1_URI)
                .setTopic("messages")
                .setClientName("processed-queue-message-publisher")
                .setSerializationSchema(new SimpleStringSchema())
                .build());

        env.fromSource(queueConsumer, "Message String Source")
            .process(new OneInputStreamProcessFunction<ReplicationMessage, String>() {
                @Override
                public void processRecord(ReplicationMessage record, Collector<String> output, PartitionedContext<String> ctx) {
                    output.collect(record.toString());
                }
            })
            .toSink(strSink);

        Thread consoleSub = new Thread(new SubscriberRunnable(
            AMPS_1_URI,
            "messages",
            "",
            "",
            "",
            "subscribe"
        ));
        if (PRINT_TO_CONSOLE) {
            consoleSub.start();
        }
 
        try {
            env.execute("Queue Replication Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            consoleSub.interrupt();
        }
    }

    public static class ReplicationMessage {
        public long counter = 0;
        public int group = 0;
        public String data = ReplicationExample.DATA;
    
        public ReplicationMessage() {}
    
        public ReplicationMessage(long c, int g, String d) {
            counter = c;
            group = g;
            data = d;
        }
    
        @Override
        public String toString() {
            return "{\"counter\":" + counter + ",\"group\":" + group + ",\"data\":\"" + data + "\"}";
        }
    }
    
    public static class ModifiedMessage {
        public long counter = 0;
        public int group = 0;
        public String data = ReplicationExample.DATA;
        public Timestamp ts = null;
    
        public ModifiedMessage() {}
    
        public ModifiedMessage(long c, int g, Timestamp timestamp, String d) {
            counter = c;
            group = g;
            ts = timestamp;
            data = d;
        }
    
        @Override
        public String toString() {
            return "{\"counter\":" + counter + ",\"group\":" + group + "\",\"data\":\"" + data + ",\"ts\":\"" + ts.toString() + "\"}";
        }
    }
    
    public static class ReplicationMessageGenerator extends DataGeneratorSource<ReplicationMessage> {
        public ReplicationMessageGenerator(long count, RateLimiterStrategy rls) {
            super(new ReplicationMessageGeneratorFunction(), count, rls, TypeInformation.of(ReplicationMessage.class));
        }
    }
    
    public static class ReplicationMessageGeneratorFunction implements GeneratorFunction<Long, ReplicationMessage> {
        @Override
        public ReplicationMessage map(Long value) throws Exception {
            ReplicationMessage rm = new ReplicationMessage();
    
            rm.counter = value;
            rm.group = (int) Math.ceil(Math.random() * 9);
    
            return rm;
        }
    }
    
    public static class ReplicationServerChooserSupplier implements ServerChooserSupplier {
        private String[] uris;
    
        public ReplicationServerChooserSupplier() {}
    
        public ReplicationServerChooserSupplier(String[] uris) {
            this.uris = uris;
        }
    
        @Override
        public ServerChooser get() {
            DefaultServerChooser sc = new DefaultServerChooser();
    
            for (int i = 0; i < uris.length; i++) {
                sc.add(uris[i]);
            }
    
            return sc;
        }
    }
}

