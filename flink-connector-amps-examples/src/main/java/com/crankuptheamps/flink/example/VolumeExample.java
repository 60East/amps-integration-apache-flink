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
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * An example of a flink streaming job that reads from AMPS and publishes to AMPS.
 *
 * <p>This example is meant to stress test the source and sink.</p>
 */
public class VolumeExample {
    // Replace with your AMPS connection string
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic where the incrementing messages are.
    //
    // Note: Messages should be formatted like "{\"num\":[integer], \"data\":[String]}".
    //
    // You can use publishVolumeMessages() to create some messages in the format
    private static final String TOPIC_FLINK_SOURCE = Constants.TOPIC.TXLOG_INC;
    // Replace with your AMPS topic where the sink messages will be published
    private static final String TOPIC_FLINK_SINK = "volume-adhoc";
    // Replace with desired bookmark
    private static final String BOOKMARK = "0";
    // Replace to change parallelism
    public static final int PARALLELISM = 1;
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 50000000;

    public static void main(String[] args) {
        List<String> splits = new ArrayList<>();

        int parallelism = Constants.getParallelism(args, PARALLELISM);
        int splitAmount = Constants.getSplitAmount(args, parallelism);

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
            .setInternalBufferSize(1000)
            .build()
            ;
        
        AMPSSink<String> ampsSink = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new SimpleStringSchema())
            .build()
            ;
        
        publishVolumeMessages(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        // Do a volume test
        volumeTest(ampsDataSource, ampsSink, parallelism);
    }

    private static <T> void volumeTest(AMPSSource<T> source, AMPSSink<T> sink, int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        try {
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source").sinkTo(sink);

            System.out.println("Submitting job to Flink. Check the Galvanometer and Flink web UI for details.");
            env.execute("Volume Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void publishVolumeMessages(int amount) {
        Thread t = new Thread(new PublisherRunnable(
            "volume-messages-pub",
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

