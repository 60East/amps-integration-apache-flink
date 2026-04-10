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

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.connector.dsv2.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * An example of a flink streaming job that intentionally fails
 * to trigger checkpoint recovery
 */
public class CheckpointExample {
    // Replace with your AMPS connection string 
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic where the incrementing messages are.
    //
    // Note: Messages should be formatted like "{\"num\":[integer], \"data\":[String]}".
    //
    // You can use publishCheckpointMessages() to create some messages in the format
    private static final String TOPIC_FLINK_SOURCE = Constants.TOPIC.TXLOG_CHECKPOINTING;
    // Replace with your AMPS topic where the sink messages will be published
    private static final String TOPIC_FLINK_SINK = "checkpoint-adhoc";
    // Replace with your AMPS options.
    // 
    // Example uses a low rate to help show the failure and recovery point
    private static final String OPTIONS = "rate=5," + Message.Options.Timestamp;
    // Replace to change parallelism
    private static final int PARALLELISM = 1;
    // The message count to trigger an intentional exception for checkpointing
    private static final int MESSAGE_TO_FAIL_AT = 100;
    // The message count required to print
    //
    // Example uses 1 so each message is printed
    private static final int MESSAGES_PER_PRINT = 1;
    // Replace to change checkpointing interval
    private static final int CHECKPOINTING_INTERVAL = 2000;
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 2500;

    public static void main(String[] args) {
        List<String> splits = new ArrayList<>();

        int parallelism = Constants.getParallelism(args, PARALLELISM);
        int splitAmount = Constants.getSplitAmount(args, parallelism);

        for (int i = 0; i < splitAmount; i++) {
            splits.add("/num MOD " + splitAmount + " = " + i);
        }

        AMPSSource<String> source = AMPSSource.<String>builder()
            .setClientName("checkpointing-source")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SOURCE)
            .setBookmark("0")
            .setDeserializationSchema(new SimpleStringSchema())
            .setOptions(OPTIONS)
            .setSplits(splits.size() > 1 ? splits : new ArrayList<>())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
            ;
        
        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("checkpointing-sink")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new SimpleStringSchema())
            .setDeliveryGuarantee(DeliveryGuarantee.NONE)
            .build();
        
        // Used to publish some messages that are used in the checkpoint test
        publishCheckpointMessages(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        AMPSCheckpointing(source, sink, parallelism);

        //datastreamV2Checkpointing(source, sink, parallelism);
    }

    private static void AMPSCheckpointing(AMPSSource<String> source, AMPSSink<String> sink, int parallelism) {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        
        Thread t = new Thread(new SubscriberRunnable(
            URI,
            TOPIC_FLINK_SINK,
            "",
            "",
            "",
            "subscribe"
        ));
        t.start();

        ProcessFunction<String, String> ps = new CheckpointProcessFunction(MESSAGES_PER_PRINT, MESSAGE_TO_FAIL_AT);

        try {
            env.enableCheckpointing(CHECKPOINTING_INTERVAL);

            env.setParallelism(parallelism);

            env.fromSource(source, WatermarkStrategy.noWatermarks(), "AMPS Data Source Checkpointing")
                .uid("src")
                .process(ps).uid("prs").sinkTo(sink).uid("snk");

            env.execute("Checkpointing Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void datastreamV2Checkpointing(AMPSSource<String> source, AMPSSink<String> sink, int parallelism) {
        final ExecutionEnvironment env;

        try {
            env = ExecutionEnvironment.getInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CheckpointConfig cc = ((ExecutionEnvironmentImpl) env).getCheckpointCfg();
        cc.setCheckpointInterval(CHECKPOINTING_INTERVAL);
        cc.setTolerableCheckpointFailureNumber(100);

        Configuration c = ((ExecutionEnvironmentImpl) env).getConfiguration();
        c.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        c.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));

        ((ExecutionEnvironmentImpl) env).setParallelism(parallelism);

        Source<String> src = DataStreamV2SourceUtils.wrapSource(source);
        Sink<String> snk = DataStreamV2SinkUtils.wrapSink(sink);

        env.fromSource(src, "V2 Checkpointing")
            .process(new OneInputStreamProcessFunction<String, String>() {
                private int counter = 1;
                private int failCounter = 1;

                @Override
                public void processRecord(String record, org.apache.flink.datastream.api.common.Collector<String> output, PartitionedContext<String> ctx) {
                    if (failCounter >= MESSAGE_TO_FAIL_AT) {
                        output.collect(record.split(",")[0] + " (Fail)");
                        throw new RuntimeException("Intentional Exception");
                    }

                    if (counter >= MESSAGES_PER_PRINT) {
                        output.collect(record.split(",")[0]);
                    }

                    counter++;
                    failCounter++;
                }
            })
            .toSink(snk);

        Thread t = new Thread(new SubscriberRunnable(
            URI,
            TOPIC_FLINK_SINK,
            "",
            "",
            "",
            "subscribe"
        ));
        t.start();

        try {
            env.execute("V2 Checkpointing Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void publishCheckpointMessages(int amount) {
        try (Client client = new Client("checkpoint-messages-pub");) {
            System.out.printf("Publishing %d messages to AMPS%n", amount);
            
            client.connect(URI);
            client.logon();

            int pubAmount = 1;

            for (; pubAmount <= 5 && pubAmount <= amount; pubAmount++) {
                client.publish(TOPIC_FLINK_SOURCE, "{\"num\":" + pubAmount + ",\"data\":\"a\"}");
                Thread.sleep(500);
            }
            
            for (; pubAmount <= amount; pubAmount++) {
                client.publish(TOPIC_FLINK_SOURCE, "{\"num\":" + pubAmount + ",\"data\":\"a\"}");
                Thread.sleep(5);
            }
            client.publishFlush(10000L);

            System.out.println("Finished publishing messages");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class CheckpointProcessFunction extends ProcessFunction<String, String> {
        private final int messageGroupSize;
        private final int failAt;

        private int count = 1;
        private int failCount = 1;

        public CheckpointProcessFunction(int messageGroupSize, int failAt) {
            this.messageGroupSize = messageGroupSize;
            this.failAt = failAt;
        }

        public CheckpointProcessFunction() {
            this.messageGroupSize = 1;
            this.failAt = 10;
        }

        @Override
        public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out)
                throws Exception {
            if (failCount > failAt) {
                String outStr = value.split(",")[0];
                out.collect(outStr + " (Fail)");
                failCount = 0;
                throw new Exception("Intentional Exception");
            }

            if (count >= messageGroupSize) {
                String outStr = value.split(",")[0];
                out.collect(outStr);
                count = 0;
            }
            count++;
            failCount++;
        }
    }
}

