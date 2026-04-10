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

import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.sink.AMPSSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * An example of a flink streaming job that publishes to AMPS.
 */
public class AMPSSinkExample {
    // Replace with your AMPS connection string
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic
    private static final String TOPIC = Constants.TOPIC.ADHOC_MESSAGE;
    // Example message to be published to AMPS
    private static final String MESSAGE_500B = "{ \"data\" : \"" + "a".repeat(485) + "\" }";
    // The amount of messages to publish.
    // Once this amount has been published, the job will end.
    private static final int MESSAGES_TO_PUBLISH = 50000000;
    // Replace to change parallelism
    private static final int PARALLELISM = 1;

    /**
     * General testing for publishing messages to AMPS using 
     * flink sinks.
     */
    public static void main(String[] args) throws Exception {
        int parallelism = Constants.getParallelism(args, PARALLELISM);
        
        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(URI)
            .setTopic(TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        // Generates messages at a rate of 10,000 per second that will
        // be sinked to AMPS
        DataGeneratorSource<String> source1 = new SimpleDataGenerator(
            MESSAGE_500B, // Message to send
            MESSAGES_TO_PUBLISH, // Amount of messages
            RateLimiterStrategy.perSecond(10000)
            );
        
        // Generates messages at no limit that will be sinked to AMPS
        DataGeneratorSource<String> source2 = new SimpleDataGenerator(
            MESSAGE_500B, // Message to send
            MESSAGES_TO_PUBLISH, // Amount of messages
            RateLimiterStrategy.noOp()
            );

        // Generates an incrementing message at no limit that will be
        // sinked to AMPS
        //
        // Example: "{\"num\":1}" then "{\"num\":2}" then "{\"num\":3}" ...
        DataGeneratorSource<String> sourceInc = new IncDataGenerator(
            MESSAGES_TO_PUBLISH, // Amount of messages
            RateLimiterStrategy.noOp()
            );   
        
        // Replace the sink/generator as needed
        useSink(sink, source2, parallelism);
    }

    /**
     * Use a Sink to sink to AMPS
     */
    public static void useSink(Sink<String> sink, Source<String, ?, ?> source, int parallelism) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        try {
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySource")
            .sinkTo(sink);

            System.out.println("Submitting job to Flink. Check the Galvanometer and Flink web UI for details.");
            env.execute("AMPSSink Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    /**
     * Generates a datastream by repeating the same string.
     */
    public static class SimpleDataGenerator extends DataGeneratorSource<String> {
        public SimpleDataGenerator(String message, long max, RateLimiterStrategy<?> rls) {
            super(new SinkGenerator1(message), max, rls, Types.STRING);
        }
    }
    
    public static class SinkGenerator1 implements GeneratorFunction<Long, String> {
        private final String message;
    
        public SinkGenerator1(String message) {
            this.message = message;
        }
    
        @Override
        public String map(Long value) throws Exception {
            return message;
        }
    }
    
    /**
     * Generates a datastream with incrementing data in a JSON format.
     */
    public static class IncDataGenerator extends DataGeneratorSource<String> {
        public IncDataGenerator(long max, RateLimiterStrategy<?> rls) {
            super(new IncGenerator(), max, rls, Types.STRING);
        }
    }
    
    public static class IncGenerator implements GeneratorFunction<Long, String> {
        @Override
        public String map(Long value) throws Exception {
            return "{\"num\":" + (value + 1) + "}";
        }
    }
}

