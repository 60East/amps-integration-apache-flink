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

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class BlogExample {
    public static final String URI = Constants.URI.JSON;
    public static final String EVENT_TOPIC = "events";
    public static final String ALERT_TOPIC = "alerts";

    public static final int SECONDS_PER_WINDOW = 5;
    public static final int BLOCKS_MINED_PER_SECOND = 2;

    // If a player has a higher rate than this in any window, publish an alert
    public static final double MAX_NORMAL_DIAMOND_CHANCE = 0.3;

    private static final double CHEATER_RATE = 0.6;
    private static final double NORMAL_RATE = 0.025;

    public static void main(String[] args) {
        Thread p1 = new Thread(new PlayerRunnable(1, CHEATER_RATE));
        Thread p2 = new Thread(new PlayerRunnable(2, NORMAL_RATE));
        Thread p3 = new Thread(new PlayerRunnable(3, Math.random() / 2));
        Thread p4 = new Thread(new PlayerRunnable(4, Math.random() / 2.5));
        Thread p5 = new Thread(new PlayerRunnable(5, Math.random() / 3));

        // Client reads alerts from AMPS and prints them into the console
        Client alertSub = new Client("alertSub");

        try {
            alertSub.connect(URI);
            alertSub.logon();
            alertSub.subscribe(new SimpleMessageHandler(), ALERT_TOPIC, 0);

            p1.start();
            p2.start();
            p3.start();
            p4.start();
            p5.start();

            doJob();
            //doJobAMPSTimestamp();
            //doJobV2();
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            alertSub.close();
            p1.interrupt();
            p2.interrupt();
            p3.interrupt();
            p4.interrupt();
            p5.interrupt();
        }
    }

    // The job that reads from AMPS and publishes alerts
    private static void doJob() throws Exception {
        AMPSSource<BlockBreakEvent> source = AMPSSource.<BlockBreakEvent>builder()
            .setUri(URI)
            .setTopic(EVENT_TOPIC)
            .setDeserializationSchema(new JsonDeserializationSchema<>(BlockBreakEvent.class))
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setUri(URI)
            .setTopic(ALERT_TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(
            source, 
            WatermarkStrategy
                .<BlockBreakEvent>forMonotonousTimestamps()
                .withTimestampAssigner((e, ts) -> e.timestamp), // Use timestamp from the object
            "Events subscriber")
        .keyBy((e) -> e.playerId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(SECONDS_PER_WINDOW)))
        .process(new ProcessWindowFunction<BlockBreakEvent, String, Integer, TimeWindow>() {
            @Override
            public void process(
                    Integer key, 
                    ProcessWindowFunction<BlockBreakEvent, String, Integer, TimeWindow>.Context context,
                    Iterable<BlockBreakEvent> elements,
                    Collector<String> out) throws Exception {
                int diamonds = 0;

                for (BlockBreakEvent event : elements) {
                    if (event.isDiamond) diamonds++;
                }

                if (diamonds > (int) (Math.ceil(SECONDS_PER_WINDOW * BLOCKS_MINED_PER_SECOND * MAX_NORMAL_DIAMOND_CHANCE))) {
                    double diamondsPerBlock = (double) diamonds / (SECONDS_PER_WINDOW * BLOCKS_MINED_PER_SECOND);

                    out.collect(String.format("(ALERT) Player %d collected %d diamonds (Mining %.2f diamonds per block)",
                        key, 
                        diamonds, 
                        diamondsPerBlock));
                }
            }
        })
        .sinkTo(sink);

        env.execute("AMPS Flink Blog Example");
    }
    
    // Version that uses the AMPS timestamp rather than the field from the message
    private static void doJobAMPSTimestamp() throws Exception {
        AMPSSource<BlockBreakEvent> source = AMPSSource.<BlockBreakEvent>builder()
            .setUri(URI)
            .setTopic(EVENT_TOPIC)
            .setDeserializationSchema(new JsonDeserializationSchema<>(BlockBreakEvent.class))
            .setOptions("timestamp,")
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setUri(URI)
            .setTopic(ALERT_TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(
            source, 
            WatermarkStrategy
                .<BlockBreakEvent>forMonotonousTimestamps()
                .withTimestampAssigner((e, ts) -> ts), // Use timestamp from AMPS
            "Events subscriber")
        .keyBy((e) -> e.playerId)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(SECONDS_PER_WINDOW)))
        .process(new ProcessWindowFunction<BlockBreakEvent, String, Integer, TimeWindow>() {
            @Override
            public void process(
                    Integer key, 
                    ProcessWindowFunction<BlockBreakEvent, String, Integer, TimeWindow>.Context context,
                    Iterable<BlockBreakEvent> elements,
                    Collector<String> out) throws Exception {
                int diamonds = 0;

                for (BlockBreakEvent event : elements) {
                    if (event.isDiamond) diamonds++;
                }

                if (diamonds > (int) (Math.ceil(SECONDS_PER_WINDOW * BLOCKS_MINED_PER_SECOND * MAX_NORMAL_DIAMOND_CHANCE))) {
                    double diamondsPerBlock = (double) diamonds / (SECONDS_PER_WINDOW * BLOCKS_MINED_PER_SECOND);

                    out.collect(String.format("(ALERT) Player %d collected %d diamonds (Mining %.2f diamonds per block)",
                        key, 
                        diamonds, 
                        diamondsPerBlock));
                }
            }
        })
        .sinkTo(sink);

        env.execute("AMPS Flink Blog Example");
    }
    
    // The DataStream V2 job that reads from AMPS and publishes alerts
    private static void doJobV2() throws Exception {
        AMPSSource<BlockBreakEvent> source = AMPSSource.<BlockBreakEvent>builder()
            .setUri(URI)
            .setTopic(EVENT_TOPIC)
            .setDeserializationSchema(new JsonDeserializationSchema<>(BlockBreakEvent.class))
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setUri(URI)
            .setTopic(ALERT_TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .build();

        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        env.fromSource(
            DataStreamV2SourceUtils.wrapSource(source), 
            "Events subscriber").withParallelism(1)
        .process(
            EventTimeExtension
                .<BlockBreakEvent>newWatermarkGeneratorBuilder(event -> event.timestamp)
                .periodicWatermark(Duration.ofMillis(100))
                .buildAsProcessFunction())
        .keyBy((e) -> e.playerId)
        .process(BuiltinFuncs.window(
            WindowStrategy.tumbling(
                Duration.ofSeconds(SECONDS_PER_WINDOW), 
                WindowStrategy.EVENT_TIME),
            new OneInputWindowStreamProcessFunction<BlockBreakEvent, String>() {
                @Override
                public void onTrigger(
                        org.apache.flink.datastream.api.common.Collector<String> output,
                        PartitionedContext<String> ctx,
                        OneInputWindowContext<BlockBreakEvent> windowContext) throws Exception {
                    int diamonds = 0;

                    for (BlockBreakEvent event : windowContext.getAllRecords()) {
                        if (event.isDiamond) diamonds++;
                    }

                    if (diamonds > (int) (Math.ceil(SECONDS_PER_WINDOW * BLOCKS_MINED_PER_SECOND * MAX_NORMAL_DIAMOND_CHANCE))) {
                        double diamondsPerBlock = (double) diamonds / (SECONDS_PER_WINDOW * BLOCKS_MINED_PER_SECOND);

                        output.collect(String.format("(ALERT) Player %d collected %d diamonds (Mining %.2f diamonds per block)",
                            ctx.getStateManager().getCurrentKey(), 
                            diamonds, 
                            diamondsPerBlock));
                    }
                }
            }
        ))
        .toSink(DataStreamV2SinkUtils.wrapSink(sink)).withParallelism(1);

        env.execute("AMPS Flink Blog Example V2");
    }

    public static class BlockBreakEvent {
        public int playerId;
        public long timestamp;
        public boolean isDiamond;
    
        BlockBreakEvent() {}
    
        BlockBreakEvent(int playerId, long timestamp, boolean isDiamond) {
            this.playerId = playerId;
            this.timestamp = timestamp;
            this.isDiamond = isDiamond;
        }
    
        public int getPlayerId() {
            return playerId;
        }
    }
    
    public static class PlayerRunnable implements Runnable {
        public int id;
        public double diamondChance;
    
        PlayerRunnable() {}
    
        PlayerRunnable(int id, double diamondChance) {
            this.id = id;
            this.diamondChance = diamondChance;
        }
    
        @Override
        public void run () {
            Client client = new Client("p-" + id);
            JsonSerializationSchema<BlockBreakEvent> serializer = new JsonSerializationSchema<>();
    
            try {
                client.connect(BlogExample.URI);
                client.logon();
    
                serializer.open(null);
    
                byte[] topic = BlogExample.EVENT_TOPIC.getBytes();
    
                while (true) {
                    Thread.sleep(1000 / BlogExample.BLOCKS_MINED_PER_SECOND);
    
                    BlockBreakEvent event = new BlockBreakEvent(
                        id, 
                        System.currentTimeMillis(),
                        Math.random() < diamondChance);
    
                    byte[] data = serializer.serialize(event);
                    
                    client.publish(topic, 0, topic.length, data, 0, data.length);
                }
            } catch (InterruptedException ie) {
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                client.close();
            }
        }
    }
    
    public static class SimpleMessageHandler implements MessageHandler {
        @Override
        public void invoke(Message m) {
            System.out.println(m.getData());
        }
    }
}

