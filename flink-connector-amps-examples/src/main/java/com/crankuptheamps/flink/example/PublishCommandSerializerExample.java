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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.MarketData;
import com.crankuptheamps.flink.example.helper.MarketDataGenerator;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.sink.writer.serializer.AMPSSerializationSchema;
import com.crankuptheamps.flink.util.SerializedElement;
import com.crankuptheamps.flink.util.SerializedElement.PublishCommand;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PublishCommandSerializerExample {
    // The URI used to connect to AMPS
    private static final String URI = Constants.URI.JSON;
    // The SOW topic that the sink will publish to
    private static final String TOPIC = Constants.TOPIC.SOW_MARKET_DATA;

    public static void main(String[] args) throws Exception {
        AMPSSink<MarketData> sink = AMPSSink.<MarketData>builder()
            .setClientName("publish-command-example")
            .setUri(URI)
            .setTopic(TOPIC)
            .setSerializationSchema(new MarketDataSerializer())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // One sink writer keeps every update for a symbol in the same serializer.
        env.setParallelism(1);
        MarketDataGenerator generator = new MarketDataGenerator(10000, RateLimiterStrategy.perSecond(2));
        env.fromSource(generator, WatermarkStrategy.noWatermarks(), "Market Data Generator").sinkTo(sink);

        Thread subscriber = new Thread(new SubscriberRunnable(
            URI,
            TOPIC,
            "",
            "",
            "oof",
            "sow_and_delta_subscribe"
            ));
        subscriber.start();
        try {
            env.execute("Publish Command Serializer Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            subscriber.interrupt();
            subscriber.join();
        }
    }

    public static class MarketDataSerializer implements AMPSSerializationSchema<MarketData> {

        private static final long serialVersionUID = 1L;

        private final SerializationSchema<MarketData> serializationSchema = new JsonSerializationSchema<>();
        private final SerializationSchema<Map<String, Object>> deltaSchema = new JsonSerializationSchema<>();
        private transient Map<String, MarketData> latestBySymbol;

        @Override
        public void open(SerializationSchema.InitializationContext context) throws Exception {
            serializationSchema.open(context);
            deltaSchema.open(context);
            latestBySymbol = new HashMap<>();
        }

        @Override
        public SerializedElement serialize(MarketData element, SinkWriter.Context context) {
            String symbol = element.getSymbol();
            element.data = "New publish";
            MarketData latest = latestBySymbol.get(symbol);
            SerializedElement serialized = new SerializedElement();

            if (latest == null) {
                MarketData snapshot = new MarketData(symbol, element.getBid(), element.getAsk(), element.getData());
                serialized.setData(serializationSchema.serialize(snapshot));
                serialized.setPublishCommand(PublishCommand.Publish);
                latestBySymbol.put(symbol, snapshot);
            } else if (ThreadLocalRandom.current().nextBoolean()) {
                double bid = element.getBid();
                double ask = element.getAsk();
                serialized.setData(deltaSchema.serialize(Map.of("symbol", symbol, "bid", bid, "ask", ask)));
                serialized.setPublishCommand(PublishCommand.DeltaPublish);
                latestBySymbol.put(symbol, new MarketData(symbol, bid, ask, latest.getData()));
            } else {
                serialized.setData(serializationSchema.serialize(latest));
                serialized.setPublishCommand(PublishCommand.SowDelete);
                latestBySymbol.remove(symbol);
            }

            return serialized;
        }
    }
}
