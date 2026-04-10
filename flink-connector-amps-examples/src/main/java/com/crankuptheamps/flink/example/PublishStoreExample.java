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

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.MemoryPublishStore;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.sink.AMPSSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * An example that uses a {@link PublishStoreSupplier} when publishing messages.
 *
 * This test requires starting and stopping the AMPS instances that are being published to.
 * When using the sink with a publish store, messages will not be lost during failover, but they may be repeated.
 * When using the sink without a publish store, messages may be lost during failover.
 *
 * In order to see the message loss when not using a publish store, it may be necessary to output the console
 * output to a file. This can be done by using the '>>' operator when submitting the job. For example, use
 * './bin/flink run /path/to/jar >> temp.txt' and examine the file to see the message loss.
 */
public class PublishStoreExample {
    private static final String AMPS_1_URI = Constants.URI.JSON_1;
    private static final String AMPS_2_URI = Constants.URI.JSON_2;
    private static final String TOPIC = "messages";

    private static final int PUBLISH_AMOUNT = 100000;
    private static final int PUBLISH_RATE = 100;

    public static void main(String[] args) throws Exception {
        AMPSSink<PSMessage> sinkNoPubStore = AMPSSink.<PSMessage>builder()
            .setServerChooserSupplier(() -> {
                DefaultServerChooser sc = new DefaultServerChooser();
                sc.add(AMPS_1_URI);
                sc.add(AMPS_2_URI);
                return sc;
            })
            .setTopic(TOPIC)
            .setClientName("publish-store-pub")
            .setSerializationSchema(new JsonSerializationSchema<>())
            .build();

        AMPSSink<PSMessage> sinkPubStore = AMPSSink.<PSMessage>builder()
            .setServerChooserSupplier(() -> {
                DefaultServerChooser sc = new DefaultServerChooser();
                sc.add(AMPS_1_URI);
                sc.add(AMPS_2_URI);
                return sc;
            })
            .setTopic(TOPIC)
            .setClientName("publish-store-pub")
            .setSerializationSchema(new JsonSerializationSchema<>())
            .setPublishStoreFunction((clientName) -> {
                try {
                    return new MemoryPublishStore(PUBLISH_RATE);
                } catch (Exception e) {
                    return null;
                }
            })
            .build();

        // Sink without publish store
        testPublishStore(sinkNoPubStore);

        // Sink with publish store
        //testPublishStore(sinkPubStore);
    }

    private static void testPublishStore(AMPSSink<PSMessage> snk) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<PSMessage> messageGen = new PSMessageGenerator(
            PUBLISH_AMOUNT,
            RateLimiterStrategy.perSecond(PUBLISH_RATE));

        env.setParallelism(1);

        HAClient c1 = new HAClient("c1");
        HAClient c2 = new HAClient("c2");

        try {
            DefaultServerChooser sc1 = new DefaultServerChooser();
            DefaultServerChooser sc2 = new DefaultServerChooser();

            sc1.add(AMPS_1_URI);
            c1.setServerChooser(sc1);
            sc2.add(AMPS_2_URI);
            c2.setServerChooser(sc2);

            c1.connectAndLogon();
            c2.connectAndLogon();

            c1.subscribe(new PSMessageHandler("c1"), TOPIC, 0);
            c2.subscribe(new PSMessageHandler("c2"), TOPIC, 0);

            env.fromSource(messageGen, WatermarkStrategy.noWatermarks(), "Publish Store Example")
                .sinkTo(snk);

            env.execute("Publish Store Example");
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            c1.close();
            c2.close();
        }
    }
    
    public static class PSMessage {
        public long counter = 0;
        public int group = 0;
        public String data = "a";
    
        public PSMessage() {}
    
        public PSMessage(long c, int g, String d) {
            counter = c;
            group = g;
            data = d;
        }
    
        @Override
        public String toString() {
            return "count: " + counter + " group: " + group;
        }
    }
    
    public static class PSMessageGenerator extends DataGeneratorSource<PSMessage> {
        public PSMessageGenerator(long count, RateLimiterStrategy rls) {
            super(new PSMessageGeneratorFunction(), count, rls, TypeInformation.of(PSMessage.class));
        }
    }
    
    public static class PSMessageGeneratorFunction implements GeneratorFunction<Long, PSMessage> {
        @Override
        public PSMessage map(Long value) throws Exception {
            PSMessage rm = new PSMessage();
    
            rm.counter = value;
            rm.group = (int) Math.ceil(Math.random() * 9);
    
            return rm;
        }
    }
    
    public static class PSMessageHandler implements MessageHandler {
        String clientName = "default";
    
        public PSMessageHandler(String clientName) {
            this.clientName = clientName;
        }
    
        @Override
        public void invoke(Message m) {
            System.out.println(clientName + " - " + m.getData());
        }
    }
}

