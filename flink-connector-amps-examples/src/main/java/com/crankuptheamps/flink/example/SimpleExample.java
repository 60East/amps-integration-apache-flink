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

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.PublisherRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * A simple example showcasing the AMPS Flink connectors. Adhoc messages are used
 * in this example.
 */
public class SimpleExample {
    public static final String URI = Constants.URI.JSON;
    public static final String INITIAL_TOPIC = "initTopic";
    public static final String MODIFIED_TOPIC = "modTopic";

    public static void main(String[] args) {
        Thread pubThread = new Thread(new PublisherRunnable(
            "pub-runnable",
            INITIAL_TOPIC,
            URI,
            1000000,
            1_000_000_000L, // around one message per second
            0,
            1,
            "example data"
        ));

        Client sub = new Client("sub-client");

        try {
            sub.connect(URI);
            sub.logon();

            // Subscribe to see the original messages published to AMPS
            sub.subscribe(new MessageHandler() {
                @Override
                public void invoke(Message message) {
                    System.out.println("Original: " + message.getData());
                }
            }, INITIAL_TOPIC, 0);

            // Subscribe to see the messages that Flink modifies and publishes to AMPS
            sub.subscribe(new MessageHandler() {
                @Override
                public void invoke(Message message) {
                    System.out.println("Modified: " + message.getData());
                }
            }, MODIFIED_TOPIC, 0);

            // Client that publishes the original messages to AMPS
            pubThread.start();

            doJob();
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            sub.close();
            pubThread.interrupt();
        }
    }

    // The job that reads from AMPS and publishes alerts
    private static void doJob() throws Exception {
        AMPSSource<String> source = AMPSSource.<String>builder()
            .setClientName("AMPSSource")
            .setUri(URI)
            .setTopic(INITIAL_TOPIC)
            .setDeserializationSchema(new SimpleStringSchema())
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(URI)
            .setTopic(MODIFIED_TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "AMPS Source")
            .process(new ProcessFunction<String, String>() {
                // The part of the string used to locate where the field to change is
                private final String searchStr = "data\":\"";

                @Override
                public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                    int dataPart = value.indexOf(searchStr);
                    // This is what the value of "data" will be replaced with
                    String modifiedData = "" + Math.random();
                    // Output the string with the modified data
                    out.collect(value.substring(0, dataPart + searchStr.length()) + modifiedData + "\"}");
                }
            })
            .sinkTo(sink);

        env.execute("Simple AMPS Connector Example");
    }
}

