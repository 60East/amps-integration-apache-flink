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
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * An example of a flink streaming job that reads from an AMPS topic.
 */
public class AMPSSourceExample {
    // Replace with your AMPS connection string
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic
    private static final String TOPIC = Constants.TOPIC.TXLOG_INC;
    // Replace with desired bookmark
    private static final String BOOKMARK = "0";
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 50000000;
    // Replace to change parallelism
    private static final int PARALLELISM = 1;

    /**
     * General testing for receiving messages from AMPS using
     * either flink source.
     *
     * Requires messages to have already been published to the topic TXLOG_INC.
     */
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
            .setTopic(TOPIC)
            .setBookmark(BOOKMARK)
            .setSplits(splits.size() > 1 ? splits : new ArrayList<>())
            .setDeserializationSchema(new SimpleStringSchema())
            .setSleepMillisAfterBlock(0) // Set to 1 see throughput difference when briefly sleeping after the block
            .build();
        
        publishMessages(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        useSource(ampsDataSource, parallelism);
    }

    /**
     * Method to use the FLIP-27 Source API
     */
    public static <T> void useSource(AMPSSource<T> source, int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        try {
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "AMPS Source")
                .process(new ProcessFunction<T, T>(){ 
                    @Override
                    public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> output) throws Exception {
                        output.collect(value);
                    }
                });
            
            System.out.println("Submitting job to Flink. Check the Galvanometer and Flink web UI for details."); 
            env.execute("AMPSSource Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void publishMessages(int amount) {
        Thread t = new Thread(new PublisherRunnable(
            "source-messages-pub",
            TOPIC,
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

