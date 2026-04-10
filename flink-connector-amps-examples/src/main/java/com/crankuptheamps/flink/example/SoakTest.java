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

import com.crankuptheamps.client.Message;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SoakTest {
    public static final String URI = Constants.URI.JSON;
    public static final String SUB_TOPIC = "job_sub";
    public static final String PUB_TOPIC = "job_pub";
    public static final int DATA_GENERATOR_PARALLELISM = 3;
    
    public static final int CHECKPOINT_INTERVAL = 2000;
    public static final long PUBLISH_AMOUNT = Long.MAX_VALUE;
    public static final int NO_C_PUBLISH_RATE = 100000;
    public static final int C_PUBLISH_RATE = 2000;
    public static final String DATA = "a".repeat(1000);
    public static final int PARALLELISM = 1;

    public static void main(String[] args) {
        try {
            // Soak test of the connectors without checkpointing
            doSoakTest();

            // Soak test of the connectors with checkpointing
            //doSoakTestCheckpointing();
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        }
    }

    private static void doSoakTest() throws Exception {
        List<String> splits = new ArrayList<>();

        for (int i = 0; i < PARALLELISM; i++) {
            splits.add("/num MOD " + PARALLELISM + " = " + i);
        }
        
        AMPSSource<String> source = AMPSSource.<String>builder()
            .setTopic(SUB_TOPIC)
            .setClientName(SUB_TOPIC + "-sub")
            .setUri(URI)
            .setDeserializationSchema(new SimpleStringSchema())
            .setSplits(splits.size() > 1 ? splits : new ArrayList<>())
            .setDeliveryGuarantee(DeliveryGuarantee.NONE)
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setUri(URI)
            .setTopic(PUB_TOPIC)
            .setClientName(PUB_TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .setDeliveryGuarantee(DeliveryGuarantee.NONE)
            .build();
        
        AMPSSink<SoakPojo> pojoSink = AMPSSink.<SoakPojo>builder()
            .setUri(URI)
            .setTopic(SUB_TOPIC)
            .setClientName(SUB_TOPIC + "-pub")
            .setSerializationSchema(new JsonSerializationSchema<>())
            .setDeliveryGuarantee(DeliveryGuarantee.NONE)
            .build();

        DataGeneratorSource<SoakPojo> pojoSource = new SoakMessageGenerator(
            PUBLISH_AMOUNT,
            RateLimiterStrategy.perSecond(NO_C_PUBLISH_RATE));
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        env.fromSource(pojoSource, WatermarkStrategy.noWatermarks(), "Create Data").setParallelism(DATA_GENERATOR_PARALLELISM)
            .sinkTo(pojoSink).setParallelism(DATA_GENERATOR_PARALLELISM);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Process Data")
            .sinkTo(sink);

        env.execute("Soak Test No Checkpointing");
    }

    private static void doSoakTestCheckpointing() throws Exception {
        String c = "-c";

        List<String> splits = new ArrayList<>();

        for (int i = 0; i < PARALLELISM; i++) {
            splits.add("/num MOD " + PARALLELISM + " = " + i);
        }
        
        AMPSSource<String> source = AMPSSource.<String>builder()
            .setTopic(SUB_TOPIC + c)
            .setClientName(SUB_TOPIC + c + "-sub")
            .setUri(URI)
            .setDeserializationSchema(new SimpleStringSchema())
            .setSplits(splits.size() > 1 ? splits : new ArrayList<>())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setOptions(Message.Options.Timestamp)
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setUri(URI)
            .setTopic(PUB_TOPIC + c)
            .setClientName(PUB_TOPIC + c)
            .setSerializationSchema(new SimpleStringSchema())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        
        AMPSSink<SoakPojo> pojoSink = AMPSSink.<SoakPojo>builder()
            .setUri(URI)
            .setTopic(SUB_TOPIC + c)
            .setClientName(SUB_TOPIC + c + "-pub")
            .setSerializationSchema(new JsonSerializationSchema<>())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        DataGeneratorSource<SoakPojo> pojoSource = new SoakMessageGenerator(
            PUBLISH_AMOUNT,
            RateLimiterStrategy.perSecond(C_PUBLISH_RATE));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        env.fromSource(pojoSource, WatermarkStrategy.noWatermarks(), "Create Data").setParallelism(DATA_GENERATOR_PARALLELISM)
            .sinkTo(pojoSink).setParallelism(DATA_GENERATOR_PARALLELISM);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Process Data")
            .sinkTo(sink);

        env.execute("Soak Test Checkpointing");
    }

    public static class SoakPojo {
        public long num;
        public String data;
    }

    public static class SoakMessageGenerator extends DataGeneratorSource<SoakPojo> {
        public SoakMessageGenerator(long count, RateLimiterStrategy rls) {
            super(new SoakMessageGeneratorFunction(), count, rls, TypeInformation.of(SoakPojo.class));
        }
    }
    
    public static class SoakMessageGeneratorFunction implements GeneratorFunction<Long, SoakPojo> {
        @Override
        public SoakPojo map(Long value) throws Exception {
            SoakPojo sp = new SoakPojo();
    
            sp.num = value;
            sp.data = DATA;

            return sp;
        }
    }
}

