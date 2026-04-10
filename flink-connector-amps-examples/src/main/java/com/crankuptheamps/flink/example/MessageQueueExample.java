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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.crankuptheamps.client.Message;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.OuterPOJO;
import com.crankuptheamps.flink.example.helper.POJOGenerator;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * An example of a flink streaming job that uses "at-least-once" and "at-most-once" queue
 * semantics from AMPS
 */
public class MessageQueueExample {
    // Replace with your AMPS connection string 
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic where messages from a queue are sent to subscribers
    private static final String TOPIC_FLINK_SOURCE = Constants.TOPIC.Q_MY_DATA_TO_PROCESS; 
    // Replace with your AMPS topic where messages are published to a queue
    private static final String TOPIC_FLINK_SINK = Constants.TOPIC.Q_MY_DATA;
    // Replace to change parallelism
    public static final int PARALLELISM = 1;

    public static void main(String[] args) {
        List<String> splits = new ArrayList<>();

        // For this example, use empty string splits
        // to create multiple readers with no content filter.
        // This example subscribes to a queue, so
        // this will work.
        for (int i = 0; i < PARALLELISM; i++) {
            splits.add("");
        }

        AMPSSource<OuterPOJO> sourceFast = AMPSSource.<OuterPOJO>builder()
            .setClientName("Queue consumer")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SOURCE)
            .setDeserializationSchema(new JsonDeserializationSchema<>(OuterPOJO.class))
            .setSplits(PARALLELISM > 1 ? splits : new ArrayList<String>())
            .setQueueSemantics("at-least-once") // "at-most-once" or "at-least-once"
            .setAckBatchSize(5000)
            .setAckTimeout(10000)
            .setOptions("max_backlog=25000," + Message.Options.Timestamp) // Can also set max_backlog through builder
            .build();
        
        AMPSSource<OuterPOJO> sourceSlow = AMPSSource.<OuterPOJO>builder()
            .setClientName("Queue consumer")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SOURCE)
            .setDeserializationSchema(new JsonDeserializationSchema<>(OuterPOJO.class))
            .setSplits(PARALLELISM > 1 ? splits : new ArrayList<String>())
            .setQueueSemantics("at-least-once") // "at-most-once" or "at-least-once"
            .setMaxBacklog(20) // Can also set max_backlog through options
            .setAckBatchSize(5)
            .setAckTimeout(10000)
            .setOptions(Message.Options.Timestamp)
            .build();
    
        // Sink that publishes the initial messages for the message queue
        AMPSSink<OuterPOJO> queueSink = AMPSSink.<OuterPOJO>builder()
            .setClientName("Queue publisher")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();

        // Sink that will publish the strings that are printed in the console.
        // These messages would simulate the processed messages being published to AMPS
        AMPSSink<String> strSink = AMPSSink.<String>builder()
            .setClientName("Queue sink")
            .setUri(URI)
            .setTopic("message-queue-adhoc")
            .setSerializationSchema(new SimpleStringSchema())
            .setDeliveryGuarantee(DeliveryGuarantee.NONE) // AT_LEAST_ONCE should be used over EXACTLY_ONCE in queues
            .build();

        /** 
         * Test that reads from a queue with no-op processing.
         *
         * Takes a source and an int for the amount of messages to publish to the queue
         */
        //testFastQueue(sourceFast, 2500000, queueSink);
        
        /**
         * Test that slowly reads from a queue to make it easier to read the processed messages.
         *
         * Takes a source and an int for the amount of messages to publish to the queue
         */
         testSlowQueue(sourceSlow, 200, queueSink, strSink);
         
         /**
          * Test for source recovery in a message queue when an exception occurs during processing.
          *
          * Takes a source and an int for the amount of messages to publish to the queue
          */
         //testExceptionQueue(sourceSlow, 100, strSink);
    }

    /**
     * Test a queue that has a no-op process function.
     */
    public static void testFastQueue(AMPSSource<OuterPOJO> source, int amountToPublish, AMPSSink<OuterPOJO> queueSink) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);
 
        // Generate the messages for the queue
        DataGeneratorSource<OuterPOJO> g = new POJOGenerator(amountToPublish, RateLimiterStrategy.noOp());

        try {
            env.enableCheckpointing(100);

            // Publish messages to the queue
            env.fromSource(g, WatermarkStrategy.noWatermarks(), "queue artificial data").setParallelism(1)
                .sinkTo(queueSink).setParallelism(4);

            // Create a datastream to consume from queue
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "queue testing")
                .process(new ProcessFunction<OuterPOJO, OuterPOJO>() {
                    @Override
                    public void processElement(OuterPOJO value, ProcessFunction<OuterPOJO, OuterPOJO>.Context ctx, Collector<OuterPOJO> out) {
                        // out.collect(value);
                    }
                });

            System.out.println("Check the Galvanometer and Flink web UI to see the messages being processed from the queue.");
            env.execute("Fast Queue Consumption Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    /**
     * Test a queue that has a sleep in its process function and prints to console
     */
    public static void testSlowQueue(AMPSSource<OuterPOJO> source, int amountToPublish, AMPSSink<OuterPOJO> queueSink, AMPSSink<String> strSink) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        // Generator to make messages for the queue
        DataGeneratorSource<OuterPOJO> g = new POJOGenerator(amountToPublish, RateLimiterStrategy.noOp());
        
        // Sink to publish messages to queue

        Thread t = new Thread(new SubscriberRunnable(
            URI,
            "message-queue-adhoc",
            "",
            "",
            "",
            "subscribe"
        ));
        t.start();

        try {
            // The source will ack messages based on the checkpoint interval.
            //
            // Lower intervals will have more frequent acks, but there may not be as many
            // messages being acked.
            env.enableCheckpointing(3000);

            // Publish the messages to the queue
            env.fromSource(g, WatermarkStrategy.noWatermarks(), "queue artificial data").setParallelism(1)
                .sinkTo(queueSink).setParallelism(1);
            
            // Create a datastream 
            DataStream<OuterPOJO> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "queue testing");

            // Process (or sink) the messages
            ds.process(new ProcessFunction<OuterPOJO, String>(){
                private int count = 1;

                @Override
                public void processElement(OuterPOJO value, ProcessFunction<OuterPOJO, String>.Context ctx,
                        Collector<String> out) throws Exception {
                    Thread.sleep(100);
                    out.collect(count + " " + value.getItemId() + " " + value.getId());
                    count++;
                }
            }).sinkTo(strSink);

            env.execute("Slow Queue Consumption Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    /**
     * Test for how the source recovers when encountering some kind of exception
     */
    private static void testExceptionQueue(AMPSSource<OuterPOJO> source, int amountToPublish, AMPSSink<String> strSink) {
        publishQueueMessages(amountToPublish);

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.setParallelism(PARALLELISM);

        Thread t = new Thread(new SubscriberRunnable(
            URI,
            "message-queue-adhoc",
            "",
            "",
            "",
            "subscribe"
        ));
        t.start();
        
        try {
            env.enableCheckpointing(3000);
            
            MessageQueueFailFunction ps = new MessageQueueFailFunction(1, 50, 100);

            env.fromSource(source, WatermarkStrategy.noWatermarks(), "message queue exception test")
                .process(ps)
                .sinkTo(strSink);

            env.execute("Queue with Exceptions Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    /**
     * Since testExceptionQueue will be restarting, use a separate function for the flink job
     * that publishes to the queue.
     */
    private static void publishQueueMessages(int amountToPublish) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generator to make messages for the queue
        DataGeneratorSource<OuterPOJO> g = new POJOGenerator(amountToPublish, RateLimiterStrategy.noOp());
        
        // Sink to publish messages to queue
        AMPSSink<OuterPOJO> queueSink = AMPSSink.<OuterPOJO>builder()
            .setClientName("Queue publisher")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();

        try {
            // Publish the messages to the queue
            env.fromSource(g, WatermarkStrategy.noWatermarks(), "queue artificial data").setParallelism(1)
                .sinkTo(queueSink).setParallelism(1);

            env.execute("Queue Message Creation");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    public static class MessageQueueFailFunction extends ProcessFunction<OuterPOJO, String> {
        private final int messageGroupSize;
        private final int failAt;
        private final int sleepDuration;
       
        private int count = 1;
        private int failCount = 1;
    
        public MessageQueueFailFunction(int messageGroupSize, int failAt, int sleepDuration) {
            this.messageGroupSize = messageGroupSize;
            this.failAt = failAt;
            this.sleepDuration = sleepDuration;
        }
    
        public MessageQueueFailFunction() {
            this.messageGroupSize = 1;
            this.failAt = 50;
            this.sleepDuration = 1000;
        }
    
        @Override
        public void processElement(OuterPOJO value, ProcessFunction<OuterPOJO, String>.Context ctx,
                Collector<String> out) throws Exception {        
            Thread.sleep(sleepDuration);
    
            if (failCount >= failAt) {
                String outStr = failCount + " " + value.getItemId() + " "  + value.getId() + " (Fail)"; 
                out.collect(outStr);
                throw new Exception("Intentional Exception");
            }
    
            if (count >= messageGroupSize) {
                String outStr = failCount + " " + value.getItemId() + " " +  value.getId();
                out.collect(outStr);
                count = 0;
            }
    
            count++;
            failCount++;
        }
    }
}

