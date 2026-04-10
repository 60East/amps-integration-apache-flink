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

import java.io.IOException;
import java.util.Set;

import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.MarketData;
import com.crankuptheamps.flink.example.helper.MarketDataGenerator;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;
import com.crankuptheamps.flink.source.reader.deserializer.AMPSDeserializationSchema;
import com.crankuptheamps.flink.util.AMPSMessage;
import com.crankuptheamps.flink.util.AMPSSourceHeaderKeys;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class CustomDeserializerExample {
    // The URI used to connect to AMPS
    private static final String URI = Constants.URI.JSON;
    // The SOW topic that the source will read from
    private static final String TOPIC_SOURCE = Constants.TOPIC.SOW_MARKET_DATA;
    // The topic that the sink will publish to
    private static final String TOPIC_SINK = Constants.TOPIC.ADHOC_MESSAGE;
    // The subscribe command the source will use
    private static final String COMMAND = "sow_and_subscribe";
    // The amount of messages to publish before running the job.
    // If the SOW already has messages, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 1000;

    public static void main(String[] args) throws Exception {
        AMPSSource<String> source = AMPSSource.<String>builder()
            .setClientName("AMPSSource")
            .setUri(URI)
            .setTopic(TOPIC_SOURCE)
            .setDeserializationSchema(new MarketDataDeserializer())
            .setSubscribeCommand(COMMAND)
            .setHeaderKeys(Set.of(AMPSSourceHeaderKeys.COMMAND))
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(URI)
            .setTopic(TOPIC_SINK)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        createMarketData(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        subscribeCommandTest(source, sink);
    }

    private static void subscribeCommandTest(AMPSSource<String> source, AMPSSink<String> sink) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Thread t = new Thread(new SubscriberRunnable(
            URI,
            TOPIC_SINK,
            "",
            "",
            "",
            "subscribe"
            ));
        t.start();

        if (COMMAND.equals("subscribe") || COMMAND.equals("sow_and_subscribe")) {
            AMPSSink<MarketData> pub = AMPSSink.<MarketData>builder()
                .setTopic(TOPIC_SOURCE)
                .setClientName("artificial-data")
                .setUri(URI)
                .setSerializationSchema(new JsonSerializationSchema<>())
                .build();
            
            DataGeneratorSource<MarketData> dataGen = new MarketDataGenerator(1000, RateLimiterStrategy.perSecond(2));

            env.fromSource(dataGen, WatermarkStrategy.noWatermarks(), "Artificial Data Generator").sinkTo(pub);
        }

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Subscribe Command Test")
            .process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                    Thread.sleep(100);
                    out.collect(value);
             }
            }).sinkTo(sink);

        try {
            env.execute("Custom Deserializer Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            t.interrupt();
        }
    }

    /**
     * Used to publish the necessary messages for some of the tests.
     */
    public static void createMarketData(int amount) throws Exception {
        if (amount < 1) return;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<MarketData> g = new MarketDataGenerator(amount, RateLimiterStrategy.noOp());

        AMPSSink<MarketData> sink = AMPSSink.<MarketData>builder()
            .setClientName("sow-generated-data-sink")
            .setUri(URI)
            .setTopic(TOPIC_SOURCE)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();

        env.setParallelism(1);

        // Publish messages 
        env.fromSource(g, WatermarkStrategy.noWatermarks(), "fake pojo data").sinkTo(sink);

        try {
            env.execute("Market Data Creation");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    public static class MarketDataDeserializer implements AMPSDeserializationSchema<String> {
        
        private static final long serialVersionUID = 1L;

        private final DeserializationSchema<MarketData> deserializationSchema = new JsonDeserializationSchema<>(MarketData.class);
    
        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            deserializationSchema.open(context);
        }

        @Override
        public void deserialize(AMPSMessage message, Collector<String> out) throws IOException {
            MarketData temp;

            // message.getCommand() can be used to use the Message.Command.x constants instead of Strings
            switch (message.getHeader(AMPSSourceHeaderKeys.COMMAND)) {
                case "group_begin":
                    out.collect("Begin SOW (group_begin)");
                    break;
                case "sow":
                    temp = deserializationSchema.deserialize(message.getDataRaw());
                    out.collect("SOW: " + temp);
                    break;
                case "group_end":
                    out.collect("End SOW (group_end)");
                    break;
                case "publish":
                    temp = deserializationSchema.deserialize(message.getDataRaw());
                    out.collect("Publish: " + temp);
                    break;
                default:
                    out.collect("Unexpected command '" + message.getHeader(AMPSSourceHeaderKeys.COMMAND) + "'");
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}

