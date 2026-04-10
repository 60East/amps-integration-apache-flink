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
import java.util.List;

import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.PublisherRunnable;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * An example of a batch flink streaming job from an AMPS topic.
 */
public class BatchExample {
    // Replace with your AMPS connection string 
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic
    private static final String TOPIC_FLINK_SOURCE = Constants.TOPIC.TXLOG_INC;
    // Replace with your AMPS topic that the sink should publish to
    private static final String TOPIC_FLINK_SINK = "batch-adhoc";
    // Replace with the amount of messages you want to read from AMPS
    private static final int TOP_N = 100;
    // Replace with the amount of messages you want to read from AMPS for the volume test
    private static final int TOP_N_VOLUME = 1000000;
    // Replace with desired options
    private static final String OPTIONS = "rate=20";
    // Replace with desired parallelism
    private static final int PARALLELISM = 1;
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = TOP_N;

    public static void main(String[] args) {
        List<String> splits = new ArrayList<>();

        for (int i = 0; i < PARALLELISM; i++) {
            splits.add("/num MOD " + PARALLELISM + " = " + i);
        }

        AMPSSource<String> source = AMPSSource.<String>builder()
            .setClientName("batch-options-source")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SOURCE)
            .setTopN(TOP_N)
            .setBookmark("0")
            .setOptions(OPTIONS)
            .setDeserializationSchema(new SimpleStringSchema())
            .setSplits(PARALLELISM > 1 ? splits : new ArrayList<>())
            .build();

        AMPSSource<String> volumeSource = AMPSSource.<String>builder()
            .setClientName("batch-source")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SOURCE)
            .setTopN(TOP_N_VOLUME)
            .setBookmark("0")
            .setDeserializationSchema(new SimpleStringSchema())
            .setSplits(PARALLELISM > 1 ? splits : new ArrayList<>())
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        /**
         * Used to publish messages for the batch job.
         */
        publishBatchMessages(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        /**
         * Used to test a small batch job from AMPS with topN defined.
         *
         * Since the messages are printed to console, OPTIONS should limit the rate
         */
        topNTest(source, sink);

        /**
         * Used to volume test a large batch job from AMPS.
         */
        //topNVolumeTest(volumeSource);
    }

    /**
     * Test meant for small top n values and slow rates.
     */
    private static void topNTest(AMPSSource<String> source, AMPSSink<String> sink) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

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
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "AMPS connector batch test")
            .process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                    out.collect(value.split(",")[0]);
                }
            })
            .sinkTo(sink);

            env.execute("TopN Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    /**
     * Test for a large top N value with no rate limiting AMPS
     */
    private static void topNVolumeTest(AMPSSource<String> source) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        try {
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "AMPS connector volume batch test")
            .process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                    //NO-OP
                }
            });

            System.out.println("Submitting job to Flink. This job will finish if there are enough messages in the transaction log.");
            env.execute("TopN Volume Example");
            System.out.println("The job should have 'Records Received' at " + TOP_N_VOLUME + ".");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void publishBatchMessages(int amount) {
        Thread t = new Thread(new PublisherRunnable(
            "batch-messages-pub",
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
}

