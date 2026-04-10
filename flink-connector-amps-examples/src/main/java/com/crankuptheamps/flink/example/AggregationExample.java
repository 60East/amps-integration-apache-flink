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

import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.MarketData;
import com.crankuptheamps.flink.example.helper.MarketDataGenerator;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

/**
 * An example of a flink streaming job that uses aggregation.
 */
public class AggregationExample {
    // Replace with your AMPS connection string
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic where the MarketData messages are.
    // You can use createMarketData() to create some messages in the format
    private static final String TOPIC_TXLOG_SOURCE = Constants.TOPIC.TXLOG_MARKET_DATA;
    // Replace with your AMPS topic where the initial MarketData adhoc messages will be published
    private static final String TOPIC_ADHOC_SOURCE = "agg-source-adhoc";
    // Replace with your AMPS topic where the sink messages will be published
    private static final String TOPIC_FLINK_SINK = "agg-sink-adhoc";
    // Replace with desired bookmark
    private static final String BOOKMARK = "0";
    // Parallelism above 1 will cause the output in the console to be mixed
    private static final int PARALLELISM = 1;
    // How many ad hoc messages should be generate per second
    private static final int GENERATED_DATA_PER_SECOND = 200;
    // The amount of seconds per window
    private static final int SECONDS_PER_WINDOW = 5;
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 0;

    public static void main(String[] args) throws Exception {
        List<String> splits = new ArrayList<>();

        for (int i = 0; i < PARALLELISM; i++) {
            splits.add("CRC32(/symbol) MOD " + PARALLELISM + " = " + i);
        }

        AMPSSource<MarketData> bookmarkSource = AMPSSource.<MarketData>builder()
            .setClientName("bk-agg-source")
            .setUri(URI)
            .setTopic(TOPIC_TXLOG_SOURCE)
            .setDeserializationSchema(new JsonDeserializationSchema<>(MarketData.class))
            .setBookmark(BOOKMARK)
            .setSplits(PARALLELISM > 1 ? splits : new ArrayList<>())
            .setOptions("timestamp")
            .build()
            ;
        
        AMPSSource<MarketData> adhocSource = AMPSSource.<MarketData>builder()
            .setClientName("adhoc-agg-source")
            .setUri(URI)
            .setTopic(TOPIC_ADHOC_SOURCE)
            .setDeserializationSchema(new JsonDeserializationSchema<>(MarketData.class))
            .setSplits(PARALLELISM > 1 ? splits : new ArrayList<>())
            .setOptions("timestamp")
            .build()
            ;
        
        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("agg-sink")
            .setUri(URI)
            .setTopic(TOPIC_FLINK_SINK)
            .setSerializationSchema(new SimpleStringSchema())
            .build()
            ;
        
        createMarketData(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        //bookmarkSubAgg(bookmarkSource, sink);

        adhocAgg(adhocSource, sink);
    }

    private static void bookmarkSubAgg(AMPSSource<MarketData> source, AMPSSink<String> sink) {
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
            env.fromSource(
                source, 
                WatermarkStrategy
                    .<MarketData>forMonotonousTimestamps()
                    .withTimestampAssigner((e, ts) -> ts), 
                "Source")
            .keyBy(MarketData::getSymbol)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(SECONDS_PER_WINDOW)))
            .aggregate(new MarketDataAggFunction())
            .sinkTo(sink);

            env.execute("Bookmark Subscription Aggregation Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            t.interrupt();
        }
    }

    private static void adhocAgg(AMPSSource<MarketData> source, AMPSSink<String> sink) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);
        
        DataGeneratorSource<MarketData> g = new MarketDataGenerator(Integer.MAX_VALUE, RateLimiterStrategy.perSecond(GENERATED_DATA_PER_SECOND));
      
        AMPSSink<MarketData> dataSink = AMPSSink.<MarketData>builder()
            .setClientName("generated-agg-data-sink")
            .setUri(URI)
            .setTopic(TOPIC_ADHOC_SOURCE)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();
        
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
            env.fromSource(g, WatermarkStrategy.noWatermarks(), "fake pojo data").setParallelism(1)
                .sinkTo(dataSink).setParallelism(1);

            env.fromSource(
                source, 
                WatermarkStrategy
                    .<MarketData>forMonotonousTimestamps()
                    .withTimestampAssigner((e, ts) -> ts), 
                "Source")
            .keyBy(MarketData::getSymbol)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(SECONDS_PER_WINDOW)))
            .aggregate(new MarketDataAggFunction())
            .sinkTo(sink);

            env.execute("Adhoc Aggregation Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            t.interrupt();
        }
    }

    public static class MarketDataAggFunction implements AggregateFunction<MarketData, MarketDataAgg, String> {                
        @Override
        public MarketDataAgg createAccumulator() {
            return new MarketDataAgg(null, 0, 0, 0);
        }

        @Override
        public MarketDataAgg add(MarketData md, MarketDataAgg mda) {
            mda.symbol = md.symbol;
            mda.bidAgg += md.bid;
            mda.askAgg += md.ask;
            mda.records++;

            return mda;
        }

        @Override
        public String getResult(MarketDataAgg mda) {
            return mda.toString();
        }

        @Override
        public MarketDataAgg merge(MarketDataAgg mda1, MarketDataAgg mda2) {
            if (!mda1.symbol.equals(mda2.symbol)) {
                throw new RuntimeException("Symbol mismatch: " + mda1.symbol + " vs " + mda2.symbol);
            }

            return new MarketDataAgg(
                mda1.symbol, 
                mda1.bidAgg + mda2.bidAgg, 
                mda1.askAgg + mda2.askAgg, 
                mda1.records + mda2.records);
        }
    }

    public static class MarketDataAgg {
        public String symbol;
        public double bidAgg = 0;
        public double askAgg = 0;
        public int records = 0;

        public MarketDataAgg(String symbol, double bidAgg, double askAgg, int records) {
            this.symbol = symbol;
            this.bidAgg = bidAgg;
            this.askAgg = askAgg;
            this.records = records;
        }

        @Override
        public String toString() {
            if (symbol == null) return "MarketDataAgg has no symbol";
            if (records == 0) return "MarketDataAgg has no records";

            return String.format("%-5s - bidAgg: %.2f, bidAvg: %.2f, askAgg: %.2f, askAvg: %.2f, records: %d",
                symbol,
                bidAgg,
                bidAgg / records,
                askAgg,
                askAgg / records,
                records);
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
            .setClientName("agg-market-data-sink")
            .setUri(URI)
            .setTopic(TOPIC_TXLOG_SOURCE)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();

        env.setParallelism(3);

        System.out.printf("Publishing %d messages to AMPS%n", amount);
        // Publish messages 
        env.fromSource(g, WatermarkStrategy.noWatermarks(), "fake pojo data").setParallelism(1).sinkTo(sink);

        env.execute("Market Data Creation");
        System.out.println("Finished publishing messages");
    }
}

