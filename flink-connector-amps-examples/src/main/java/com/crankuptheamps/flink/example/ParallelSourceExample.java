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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.PublisherRunnable;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * An example of a flink streaming job that uses parallelism to read from AMPS.
 *
 * <p>This example is meant to show that no messages are lost and that messages
 * are received in order from each split. It is not meant to show the performance
 * increase when using parallelism. Use VolumeExample.java and change PARALLELISM to
 * see the performance increase when using parallelism. This example demonstrates that
 * no messages are lost when using parallelism using a bookmark subscription. If the numbers 1-9 were
 * stored in a txlog and the splits used MOD 3, they will always arrive in the
 * order they were stored in the txlog. 1, 4, 7 MOD 3 = 1, so they will always
 * arrive in the order 1, 4, 7. Messages from other splits may arrive first,
 * but the order will always be preserved.</p>
 *
 * <p>The "Missing" counter shows how many messages haven't been seen yet.
 * It should be close to PARALLELISM or 0. If the txlog is deleted then republished to,
 * the counter could be different from PARALLELISM, for there may be
 * gaps in the incrementing messages.</p>
 *
 * <p>If the counter is not increasing with every print, then messages are arriving as intended,</p>
 */
public class ParallelSourceExample {
    // Replace with your AMPS connection string
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic where the incrementing messages are.
    //
    // Note: Messages should be formatted like "{\"num\":[integer], \"data\":[String]}".
    // 
    // You can use publishParallelMessages() to create some messages in the format
    private static final String TOPIC_FLINK_SOURCE = Constants.TOPIC.TXLOG_INC;
    // Replace with your AMPS topic where the sink messages will be published
    private static final String TOPIC_FLINK_SINK = "parallel-source-adhoc";
    // Replace to change how often messages are sent to the sink
    private static final int MESSAGE_GROUP_SIZE = 500000;
    // Replace with desired bookmark
    private static final String BOOKMARK = "0";
    // Replace to change parallelism
    public static final int PARALLELISM = 2;
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 10000000;

    public static void main(String[] args) {
        List<String> splits = new ArrayList<>();

        int parallelism = Constants.getParallelism(args, PARALLELISM);
        int splitAmount = Constants.getSplitAmount(args, parallelism);

        // Splits should partition messages from AMPS equally
        for (int i = 0; i < splitAmount; i++) {
            splits.add("/num MOD " + splitAmount + " = " + i);
        }

        AMPSSource<String> ampsDataSource = AMPSSource.<String>builder()
            .setClientName("AMPSSource")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SOURCE)
            .setDeserializationSchema(new SimpleStringSchema())
            .setBookmark(BOOKMARK)
            .setSplits(splits.size() > 1 ? splits : new ArrayList<>())
            .build()
            ;

        AMPSSink<String> ampsSink2 = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new SimpleStringSchema())
            .build()
            ;

        publishParallelMessages(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        // Starts the test
        parallelTest(ampsDataSource, ampsSink2, splitAmount, parallelism);
    }
    
    private static void parallelTest(AMPSSource<String> source, AMPSSink<String> sink, int splitAmount, int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        try {
            DataStream<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "ampssource");

            ds.process(new CheckingProcessFunction<>(MESSAGE_GROUP_SIZE, splitAmount)).setParallelism(1).sinkTo(sink).setParallelism(1);

            Thread t = new Thread(new SubscriberRunnable(
              URI,
              TOPIC_FLINK_SINK,
              "",
              "",
              "",
              "subscribe"
              ));
            t.start();

            env.execute("Parallelism Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void publishParallelMessages(int amount) {
        Thread t = new Thread(new PublisherRunnable(
            "parallel-messages-pub",
            TOPIC_FLINK_SOURCE,
            URI,
            amount,
            0,
            1,
            1
        ));

        try {
            System.out.printf("Publishing %d messages to AMPS%n", amount);
            t.start();
            t.join();
            System.out.println("Finished publishing messages");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class CheckingProcessFunction<T> extends ProcessFunction<T, String> {
        private final int messageGroupSize;
        private final int splitAmount;
        private final HashSet<Long> expected = new HashSet<>();

        private int count = 1;

        public CheckingProcessFunction() {
            this.splitAmount = PARALLELISM;
            this.messageGroupSize = 1000;
        }

        public CheckingProcessFunction(int messageGroupSize, int splitAmount) {
            this.messageGroupSize = messageGroupSize;
            this.splitAmount = splitAmount;
        }

        @Override
        public void processElement(T value, ProcessFunction<T, String>.Context ctx, Collector<String> out)
                throws Exception {
            long num;

            try {
                num = Long.parseLong(value.toString().split(",")[0].split(":")[1]);
            } catch (NumberFormatException e) {
                return;
            }

            expected.add(num + splitAmount);
            expected.remove(num);
            
            if (count >= messageGroupSize) {
                out.collect("Missing: " + (expected.size() - splitAmount) + " at " + num);

                count = 1;
            } else {
                count++;
            }
        }
    }
}

