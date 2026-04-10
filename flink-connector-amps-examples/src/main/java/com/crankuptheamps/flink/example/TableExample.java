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
import com.crankuptheamps.flink.example.helper.MarketData;
import com.crankuptheamps.flink.example.helper.MarketDataGenerator;
import com.crankuptheamps.flink.example.helper.SubscriberRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Several examples of flink streaming jobs with the table API and the AMPS connectors.
 */
public class TableExample {
    // Replace with your AMPS connection string
    private static final String URI = Constants.URI.JSON;
    // Replace with your AMPS topic for the market data messages
    private static final String TXLOG_MARKET_DATA = Constants.TOPIC.TXLOG_MARKET_DATA;
    // Replace with your AMPS topic where adhoc messages for this test will be published
    private static final String ADHOC_MESSAGES = Constants.TOPIC.ADHOC_MESSAGE;
    // The amount of messages to publish before running the job.
    // If the job only involves adhoc messages or enough messages are already in the transaction log, this can be set to 0.
    private static final int MESSAGES_TO_PUBLISH = 0;

    public static void main(String[] args) throws Exception {
        createMarketData(Constants.getPublishAmount(args, MESSAGES_TO_PUBLISH));

        /**
         * Slowly read messages from AMPS into the table API.
         *
         * Shows each individual change for each symbol.
         */
        //slowBookmarkMessages();
        
        /**
         * Publish and receive adhoc messages to emit into the table API.
         *
         * Uses aggregation to show the total amount of changes any symbol received.
         */
        adhocMessages();
        
        /**
         * Read large amounts of messages from AMPS into the table API.
         * 
         * Uses aggregation to show the total amount of changes any symbol received.
         */
        //volumeMessages();
    }

    /**
     * Test to read a large amount of messages from AMPS into the table API
     */
    public static void volumeMessages() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        List<String> splits = new ArrayList<>();

        splits.add("CRC32(/symbol) MOD 4 = 0");
        splits.add("CRC32(/symbol) MOD 4 = 1");
        splits.add("CRC32(/symbol) MOD 4 = 2");
        splits.add("CRC32(/symbol) MOD 4 = 3");

        // Used to read from transaction log
        AMPSSource<MarketData> source = AMPSSource.<MarketData>builder()
            .setClientName("table-source")
            .setUri(URI)
            .setTopic(TXLOG_MARKET_DATA)
            .setBookmark("0")
            .setSplits(splits)
            .setDeserializationSchema(new JsonDeserializationSchema<>(MarketData.class))
            .build();

        // Used to publish aggregated rows to AMPS
        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("table-sink")
            .setUri(URI)
            .setTopic("agg_market")
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        // Used to read the rows published by the sink
        Thread t = new Thread(new SubscriberRunnable(
            URI,
            "agg_market",
            "",
            "",
            "",
            "subscribe"
            ));
        t.start();

        env.setParallelism(1);
        
        // Change configs to improve table performance
        TableConfig config = tableEnv.getConfig();
        config.set("table.exec.mini-batch.enabled", "true");
        config.set("table.exec.mini-batch.allow-latency", "5 s");
        config.set("table.exec.mini-batch.size", "10000");

        // Read from txlog
        DataStream<MarketData> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "volume table test").setParallelism(4);

        Table inputTable = tableEnv.fromDataStream(ds);

        Table resultTable = inputTable.groupBy($("symbol"))
                              .select($("symbol"), $("symbol").count().as("quantity"));

        tableEnv.toChangelogStream(resultTable).process(new ProcessFunction<Row, String>() {
          @Override
          public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
              out.collect(value.toString());
          }
        }).sinkTo(sink).setParallelism(1);

        try {
            env.execute("Table Example with Bookmark Subscription");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            t.interrupt();
        }
    }

    /**
     * Small test that sinks a small amount of messages from the table API to AMPS
     */
    public static void adhocMessages() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Generates data
        DataGeneratorSource<MarketData> g = new MarketDataGenerator(1000, RateLimiterStrategy.perSecond(1));

        // Sink to AMPS with the generated data
        AMPSSink<MarketData> sink = AMPSSink.<MarketData>builder()
            .setClientName("table-generated-data-sink")
            .setUri(URI)
            .setTopic(ADHOC_MESSAGES)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();
        
        // Read the generated data from AMPS
        AMPSSource<MarketData> source = AMPSSource.<MarketData>builder()
            .setClientName("table-source")
            .setUri(URI)
            .setTopic(ADHOC_MESSAGES)
            .setDeserializationSchema(new JsonDeserializationSchema<>(MarketData.class))
            .build();

        // Used to sink the aggregated table data to AMPS
        AMPSSink<String> rowAggSink = AMPSSink.<String>builder()
            .setClientName("table-sink")
            .setUri(URI)
            .setTopic("agg_market")
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        // Used to read the rows published by the sink
        Thread t = new Thread(new SubscriberRunnable(
            URI,
            "agg_market",
            "",
            "",
            "",
            "subscribe"
            ));
        t.start();

        env.setParallelism(1);

        // Publish generated messages 
        DataStream<MarketData> dataStream = env.fromSource(g, WatermarkStrategy.noWatermarks(), "fake pojo data");
        dataStream.sinkTo(sink);

        // Receive generated messages
        DataStream<MarketData> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "general");

        // Create table from messages
        Table inputTable = tableEnv.fromDataStream(ds);

        // Create result table
        Table resultTable = inputTable.groupBy($("symbol"))
                                    .select($("symbol"), $("bid").count().as("quantity"));

        // Process changelog stream  
        tableEnv.toChangelogStream(resultTable).process(new ProcessFunction<Row,String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row,String>.Context ctx, Collector<String> out)
                    throws Exception {
                out.collect(value.toString()); 
            }
        }).sinkTo(rowAggSink);

        try {
            env.execute("Table Example with Adhoc Messages");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            t.interrupt();
        }
    }

    /**
     * Test that reads from the txlog slowly and aggregates the messages in the table API
     */
    public static <T> void slowBookmarkMessages() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        AMPSSource<MarketData> ampsSource = AMPSSource.<MarketData>builder()
            .setClientName("table-source")
            .setUri(URI)
            .setTopic(TXLOG_MARKET_DATA)
            .setBookmark("0")
            .setOptions("rate=1")
            .setDeserializationSchema(new JsonDeserializationSchema<>(MarketData.class))
            .build();

        env.setParallelism(1);

        // Used to sink the aggregated table data to AMPS
        AMPSSink<String> rowAggSink = AMPSSink.<String>builder()
            .setClientName("table-sink")
            .setUri(URI)
            .setTopic("agg_market")
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        // Used to read the rows published by the sink
        Thread t = new Thread(new SubscriberRunnable(
            URI,
            "agg_market",
            "",
            "",
            "",
            "subscribe"
            ));
        t.start();

        // Receive messages
        DataStream<MarketData> ds = env.fromSource(ampsSource, WatermarkStrategy.noWatermarks(), "general");

        // Create table from messages
        Table inputTable = tableEnv.fromDataStream(ds);

        // Create table based on aggregation of messages
        Table resultTable = inputTable.groupBy($("symbol"))
                                        .select($("symbol"), $("bid").sum().as("bid"), $("ask").sum().as("ask"));

        // Process changelog stream 
        tableEnv.toChangelogStream(resultTable).process(new ProcessFunction<Row,String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out)
                    throws Exception {
                out.collect(value.toString());
            }
        }).sinkTo(rowAggSink);

        try {
            env.execute("Table Example with Slow Bookmark Subscription");
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
            .setClientName("table-generated-data-sink")
            .setUri(URI)
            .setTopic(TXLOG_MARKET_DATA)
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();

        env.setParallelism(3);

        // Publish messages 
        DataStream<MarketData> dataStream = env.fromSource(g, WatermarkStrategy.noWatermarks(), "fake pojo data").setParallelism(1);
        dataStream.sinkTo(sink);

        System.out.printf("Publishing %d messages to AMPS%n", amount);
        env.execute("Market Data Creation");
        System.out.println("Finished publishing messages");
    }
}

