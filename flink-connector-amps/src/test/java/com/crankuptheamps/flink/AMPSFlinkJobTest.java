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

package com.crankuptheamps.flink;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.testutils.AMPSMessageHandler;
import com.crankuptheamps.flink.testutils.AMPSPojo;
import com.crankuptheamps.flink.testutils.AMPSPojoDataGenerator;
import com.crankuptheamps.flink.testutils.AMPSPublisherRunnable;
import com.crankuptheamps.flink.testutils.AMPSStringDataGenerator;
import com.crankuptheamps.flink.testutils.TestConstants;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSFlinkJobTest {
    
    @RegisterExtension
    public static MiniClusterExtension flinkCluster =
        new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .setConfiguration(new Configuration()
                    .set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay")
                    .set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 5)
                    .set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100))
                    .set(CoreOptions.DEFAULT_PARALLELISM, 1)
                )
                .build());

    @BeforeAll
    public static void initIfAMPSRunning() {
        try {
            URL ampsUrl = new URL(TestConstants.URL);

            HttpURLConnection c = (HttpURLConnection) ampsUrl.openConnection();
            c.setRequestMethod("GET");
            c.connect();

            assertEquals(200, c.getResponseCode());
        } catch (Exception e) {
            throw new RuntimeException("Exception when connecting to AMPS", e);
        }
    }

    @AfterAll
    public static void cleanUp() {
        try {
            String tempDirStr = System.getProperty("java.io.tmpdir");

            Path tempDir = Paths.get(tempDirStr);
            String regex = "^junit\\d+$";
            Pattern pattern = Pattern.compile(regex);

            if (!Files.exists(tempDir) || !Files.isDirectory(tempDir)) {
                return;
            }

            File[] files = tempDir.toFile().listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory() && pattern.matcher(file.getName()).matches()) {
                        try (Stream<Path> walk = Files.walk(file.toPath());) {
                            walk.sorted(Comparator.reverseOrder())
                                .forEach(p -> {
                                    try {
                                        Files.delete(p);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception when cleaning up after Flink job tests");
            e.printStackTrace();
        }
    }

    @Nested
    public class DataStreamV1 {
        protected static final String PREFIX = "testV1";

        @Nested
        public class BasicSourceSinkTest {
            private static LinkedBlockingQueue<String> values;
            
            @BeforeEach
            public void resetValues() {
                values = new LinkedBlockingQueue<>(100);
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testAMPSSource() throws Exception {
                final String topic = PREFIX + "AMPSSource";
                
                final String data = getData();
                final int publisherSleep = 1;
                final int publisherMax = 5;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setContentFilter("/data = '" + data + "'")
                    .setBookmark("0")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                Thread t = new Thread(new AMPSPublisherRunnable(
                    TestConstants.URI,
                    topic,
                    data,
                    publisherSleep,
                    publisherMax));
                
                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(value);
                            }
                        });

                    jc = env.executeAsync(topic);

                    t.start();
                    t.join();

                    Set<String> messages = new HashSet<>();

                    while (messages.size() < publisherMax) {
                        messages.add(values.take());
                    }

                    assertEquals(publisherMax, messages.size(), "All intended messages should arrive");
                    for (int i = 0; i < publisherMax; i++) {
                        assertTrue(
                            messages.contains(String.format(AMPSPublisherRunnable.MESSAGE_FORMAT, i, data)), 
                            "Should have intended message");
                    }
                } finally {
                    t.interrupt();
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testAMPSSink() throws Exception {
                final String topic = PREFIX + "AMPSSink";
                final int max = 5;

                final String data = getData();

                DataGeneratorSource<String> source = new AMPSStringDataGenerator(max, data, RateLimiterStrategy.perSecond(max * 2));
                
                AMPSSink<String> sink = AMPSSink.<String>builder()
                    .setTopic(topic)
                    .setClientName(topic)
                    .setUri(TestConstants.URI)
                    .setSerializationSchema(TestConstants.getStringSchema())
                    .build();

                AMPSMessageHandler mh = new AMPSMessageHandler(max);

                try (Client sub = new Client(topic + "-sub");
                        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    sub.connect(TestConstants.URI);
                    sub.logon();
                    sub.bookmarkSubscribe(mh, topic, "/data = '" + data + "'", null, "0", "", 0);
                    
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .sinkTo(sink);

                    env.execute(topic);

                    Set<String> messages = new HashSet<>();

                    while (messages.size() < max) {
                        messages.add(mh.queue.take());
                    }

                    assertEquals(max, messages.size(), "All intended messages should arrive");
                    for (int i = 0; i < max; i++) {
                        assertTrue(
                            messages.contains(String.format(AMPSPublisherRunnable.MESSAGE_FORMAT, i, data)), 
                            "Should have intended message");
                    }
                }
            }
        }

        @Nested
        public class DeliveryGuaranteeTest {
            private static final int max = 10;
            private static final int failAt = max - 1;
            private static final int checkpointInterval = 500;
            private static final String rate = "rate=" + max;

            private static final Set<String> arrived = new HashSet<>();

            @BeforeEach
            public void resetValues() {
                arrived.clear();
            }
            
            @Test
            @Timeout(value = TestConstants.LONG_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testAtLeastOnceWithExceptionJob() throws Exception {
                final String topic = PREFIX + "AtLeastOnceWithExceptionJob";
                final String pubTopic = topic + "-final";

                final String data = getData();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setContentFilter("/data = '" + data + "'")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setOptions(rate)
                    .setUseSuffix(true)
                    .build();

                AMPSSink<String> sink = AMPSSink.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(pubTopic)
                    .setClientName(topic + "-pub")
                    .setSerializationSchema(TestConstants.getStringSchema())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setUseSuffix(true)
                    .build();

                JobClient jc = null;
                AMPSMessageHandler mh = new AMPSMessageHandler();

                try (Client sub = new Client(pubTopic + "-sub-" + data);
                        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    sub.connect(TestConstants.URI);
                    sub.logon();
                    sub.subscribe(mh, pubTopic, 0);

                    publishRecords(env, topic, max, data);
                    
                    env.enableCheckpointing(checkpointInterval);

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new DeliveryGuaranteeProcessFunction())
                        .sinkTo(sink);

                    jc = env.executeAsync(topic);

                    Set<String> messages = new HashSet<>();
                    List<String> allMessages = new ArrayList<>();
                    while (allMessages.size() < max * 2 && messages.size() < max) {
                        String message = mh.queue.poll(5, TimeUnit.SECONDS);
                        assertNotNull(message, "Should not have timed out during poll.");

                        allMessages.add(message);
                        messages.add(message);
                    }
                    
                    assertTrue(max * 2 > allMessages.size(), "Should be checkpointing messages.");
                    assertEquals(max, messages.size(), "Should be " + max + " unique messages.");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.LONG_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testExactlyOnceWithExceptionJob() throws Exception {
                final String topic = PREFIX + "ExactlyOnceWithExceptionJob";
                final String pubTopic = topic + "-final";

                final String data = getData();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setContentFilter("/data = '" + data + "'")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setOptions(rate + "," + Message.Options.Timestamp)
                    .setUseSuffix(true)
                    .build();

                AMPSSink<String> sink = AMPSSink.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(pubTopic)
                    .setClientName(topic + "-pub")
                    .setSerializationSchema(TestConstants.getStringSchema())
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setUseSuffix(true)
                    .build();

                JobClient jc = null;
                AMPSMessageHandler mh = new AMPSMessageHandler();

                try (Client sub = new Client(pubTopic + "-sub-" + data);
                        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    sub.connect(TestConstants.URI);
                    sub.logon();
                    sub.subscribe(mh, pubTopic, 0);

                    for (int i = 0; i < max; i++) {
                        AMPSPojo pojo = new AMPSPojo(i, data, i);
                        sub.publish(topic, pojo.toString());
                        Thread.sleep(50);
                    }
                    sub.publishFlush(10000L);
                    
                    env.enableCheckpointing(checkpointInterval);

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new DeliveryGuaranteeProcessFunction())
                        .sinkTo(sink);

                    jc = env.executeAsync(topic);

                    Set<String> messages = new HashSet<>();
                    List<String> allMessages = new ArrayList<>();
                    while (allMessages.size() < max && messages.size() < max) {
                        String message = mh.queue.poll(5, TimeUnit.SECONDS);
                        assertNotNull(message, "Should not have timed out during poll.");

                        allMessages.add(message);
                        messages.add(message);
                    }
                    
                    assertEquals(max, allMessages.size(), "Should receive exactly " + max + " messages.");
                    assertEquals(max, messages.size(), "Should be " + max + " unique messages.");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }

            private static class DeliveryGuaranteeProcessFunction extends ProcessFunction<String, String> {
                private int failCount = 0;
                
                @Override
                public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                    if (failCount < failAt) {
                        output.collect(value);
                    } else {
                        Thread.sleep(100); // Delay to let Flink store the snapshot
                        throw new Exception("Trigger checkpoint fail");
                    }

                    if (arrived.add(value)) {
                        failCount++;
                    }
                }
            }
        }

        @Nested
        public class BookmarkSubJobTest {
            private static LinkedBlockingQueue<String> values;
            
            @BeforeEach
            public void resetValues() {
                values = new LinkedBlockingQueue<>(100);
            }
            
            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testBookmarkSubWithTimestampsJob() throws Exception {
                final String topic = PREFIX + "BookmarkSubWithTimestampsJob";
                
                final int max = 1;

                AMPSSource<String> sourceTimestamps = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub-ts")
                    .setBookmark("0")
                    .setTopN(max)
                    .setOptions(Message.Options.Timestamp)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();
    
                AMPSSource<String> sourceNoTimestamps = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub-no-ts")
                    .setBookmark("0")
                    .setTopN(max)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic, max, "a");

                    env.fromSource(sourceTimestamps, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(ctx.timestamp().toString());
                            }
                        });

                    env.fromSource(sourceNoTimestamps, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(ctx.timestamp().toString());
                            }
                        });

                    env.execute(topic);
                    
                    long first = Long.parseLong(values.take());
                    long second = Long.parseLong(values.take());

                    assertTrue((first == Long.MIN_VALUE && second > 0) || (first > 0 && second == Long.MIN_VALUE), "Should have a min value long and timestamp long.");
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testBookmarkSubJob() throws Exception {
                final String topic = PREFIX + "BookmarkSubJob";
                
                final int max = 5;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic, max, "a");

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(value);
                            }
                        });

                    jc = env.executeAsync(topic);

                    List<String> messages = new ArrayList<>();
                    while (messages.size() < max) {
                        messages.add(values.take());
                    }

                    assertEquals(max, messages.size(), "Should have received messages");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }
            
            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testBookmarkSubCheckpointingJob() throws Exception {
                final String topic = PREFIX + "BookmarkSubCheckpointingJob";
                
                final String data = getData();
                final int max = 10;
                final int failAt = max - 1;
                final int checkpointInterval = 250;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setContentFilter("/data = '" + data + "'")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setUseSuffix(true)
                    .build();

                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic, max, data);

                    env.enableCheckpointing(checkpointInterval);

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            private int failCount = 0;
            
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                Thread.sleep(checkpointInterval / 5);

                                if (failCount < failAt) {
                                    values.offer(value);
                                } else {
                                    Thread.sleep(100); // Brief wait for Flink to store checkpoint
                                    throw new Exception("Trigger checkpoint fail");
                                }

                                failCount++;
                            }
                        });

                    jc = env.executeAsync(topic);

                    Set<String> messages = new HashSet<>();
                    List<String> allMessages = new ArrayList<>();
                    while (messages.size() < max && allMessages.size() < max * 2) {
                        String message = values.take();

                        allMessages.add(message);
                        messages.add(message);
                    }
                    
                    assertTrue(allMessages.size() < max * 2, "Did not properly checkpoint when resuming subscription");
                    assertEquals(max, messages.size(), "All intended messages should arrive");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testBookmarkSubTopNJob() throws Exception {
                final String topic = PREFIX + "BookmarkSubTopNJob";
                
                final int max = 5;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setTopN(max)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic, max, "a");

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(value);
                            }
                        });

                    env.execute(topic);

                    assertEquals(max, values.size(), "Should have received topN messages");
                }
            }
        }

        @Nested
        public class SOWJobTest {
            private static LinkedBlockingQueue<String> values;
            
            @BeforeEach
            public void resetValues() {
                values = new LinkedBlockingQueue<>(100);
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testSOWJob() throws Exception {
                final String topic = PREFIX + "SOWJob";
                
                final int max = 5;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setSubscribeCommand("sow")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                        Client client = new Client(topic + "-sowDelete");) {
                    client.connect(TestConstants.URI);
                    client.logon();
                    TestConstants.sowDelete(client, topic);

                    publishRecords(env, topic, max, "a");

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(value);
                            }
                        });

                    env.execute(topic);

                    assertTrue(max <= values.size(), "Should be at least max records");

                    while (values.peek() != null) {
                        assertFalse(values.poll().isBlank(), "Should not be any blank messages");
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testSOWTopNJob() throws Exception {
                final String topic = PREFIX + "SOWTopNJob";
                
                final int max = 5;
                final int topN = 3;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setSubscribeCommand("sow")
                    .setTopN(topN)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                        Client client = new Client(topic + "-sowDelete");) {
                    client.connect(TestConstants.URI);
                    client.logon();
                    TestConstants.sowDelete(client, topic);

                    publishRecords(env, topic, max, "a");

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                values.offer(value);
                            }
                        });

                    env.execute(topic);

                    assertEquals(topN, values.size(), "Should be exactly top n records");

                    while (values.peek() != null) {
                        assertFalse(values.poll().isBlank(), "Should not be any blank messages");
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testSOWAndSubscribeJob() throws Exception {
                final String topic = PREFIX + "SOWAndSubscribeJob";
                
                final int max = 5;
                final int messagesToReceive = max * 2;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setSubscribeCommand("sow_and_subscribe")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                DataGeneratorSource<AMPSPojo> generator = new AMPSPojoDataGenerator(max, RateLimiterStrategy.perSecond(100));
                
                AMPSSink<AMPSPojo> sink = AMPSSink.<AMPSPojo>builder()
                    .setTopic(topic)
                    .setClientName(topic + "-pub")
                    .setUri(TestConstants.URI)
                    .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                    .build();

                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                        Client client = new Client(topic + "-sowDelete");) {
                    client.connect(TestConstants.URI);
                    client.logon();
                    TestConstants.sowDelete(client, topic);

                    publishRecords(env, topic, max, "init");

                    env.fromSource(generator, WatermarkStrategy.noWatermarks(), topic)
                        .sinkTo(sink);

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                 values.offer(value);
                            }
                        });

                    jc = env.executeAsync(topic);

                    List<String> messages = new ArrayList<>();
                    while (messages.size() < messagesToReceive) {
                        messages.add(values.take());
                    }
                    assertEquals(messagesToReceive, messages.size(), "Should have received intended messages");

                    for (int i = 0; i < messages.size(); i++) {
                        assertFalse(messages.get(i).isBlank(), "Should not be any blank messages");
                    }
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }
        }

        @Nested
        public class ParallelismJobTest {
            private static LinkedBlockingQueue<String> values;
            private static LinkedBlockingQueue<String> values2;
            
            @BeforeEach
            public void resetValues() {
                values = new LinkedBlockingQueue<>(100);
                values2 = new LinkedBlockingQueue<>(100);
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testParallelismJob() throws Exception {
                final String topic = PREFIX + "ParallelismJob";

                final int max = 10;

                List<String> splits = new ArrayList<>();
                splits.add("/id MOD 2 = 0");
                splits.add("/id MOD 2 = 1");

                AMPSSource<AMPSPojo> source = AMPSSource.<AMPSPojo>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setTopN(max / 2) // Want 5 from each split
                    .setSplits(splits)
                    .setDeserializationSchema(TestConstants.getAMPSPojoDeserializer())
                    .build();

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic, max, "a");

                    env.setParallelism(splits.size());
                    
                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<AMPSPojo, String>() {
                            @Override
                            public void processElement(AMPSPojo value, ProcessFunction<AMPSPojo, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                if (value.id % 2 == 0) {
                                    values.offer(Long.toString(value.id));
                                } else {
                                    values2.offer(Long.toString(value.id));
                                }
                            }
                        });

                    env.execute(topic);

                    assertEquals(max, values.size() + values2.size(), "All intended messages should have arrived");
                    assertTrue(values.size() == values2.size(), "Both splits should have received an equal amount of messages");
                }
            }
            
            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testParallelismCheckpointingJob() throws Exception {
                final String topic = PREFIX + "ParallelismCheckpointingJob";

                final String data = getData();
                final int max = 10;
                final int failAt = max - 1;
                final int checkpointInterval = 250;

                String options = "rate=" + max;
                
                List<String> splits = new ArrayList<>();
                splits.add("/id MOD 2 = 0");
                splits.add("/id MOD 2 = 1");

                AMPSSource<AMPSPojo> source = AMPSSource.<AMPSPojo>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic + "-sub")
                    .setBookmark("0")
                    .setSplits(splits)
                    .setContentFilter("/data = '" + data + "'")
                    .setDeserializationSchema(TestConstants.getAMPSPojoDeserializer())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setOptions(options)
                    .build();

                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic, max, data);

                    env.setParallelism(splits.size());
                    env.enableCheckpointing(checkpointInterval);

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                        .process(new ProcessFunction<AMPSPojo, String>() {
                            private int failCount = 0;

                            @Override
                            public void processElement(AMPSPojo value, ProcessFunction<AMPSPojo, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                if (failCount < failAt) {
                                    if (value.id % 2 == 0) {
                                        values.offer(Long.toString(value.id));
                                        failCount++;
                                    } else {
                                        values2.offer(Long.toString(value.id));
                                        failCount++;
                                    }
                                } else {
                                    Thread.sleep(100); //Brief wait for Flink to store checkpoints
                                    throw new Exception("Trigger checkpoint fail");
                                }
                            }
                        });

                    jc = env.executeAsync(topic);

                    Set<String> messages = new HashSet<>();
                    Set<String> messages2 = new HashSet<>();
                    List<String> allMessages = new ArrayList<>();

                    while (messages.size() + messages2.size() < max && allMessages.size() < max * 2) {
                        String message = values.take();

                        allMessages.add(message);
                        messages.add(message);

                        message = values2.take();

                        allMessages.add(message);
                        messages2.add(message);
                    }
                    
                    assertTrue(allMessages.size() < max * 2, "Replayed too many messages");
                    assertEquals(max, messages.size() + messages2.size(), "All intended messages should arrive");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testParallelismWithAMPSSplitsJob() throws Exception {
                final String topic1 = PREFIX + "ParallelismWithAMPSSplitsTopic1Job";
                final String topic2 = PREFIX + "ParallelismWithAMPSSplitsTopic2Job";

                final int max = 10;

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic1, ""));
                splits.add(new AMPSSplit(topic2, ""));

                AMPSSource<AMPSPojo> source = AMPSSource.<AMPSPojo>builder()
                    .setUri(TestConstants.URI)
                    .setClientName(topic1 + "-sub")
                    .setBookmark("0")
                    .setTopN(max / 2) // Want 5 from each split
                    .setAMPSSplits(splits)
                    .setDeserializationSchema(TestConstants.getAMPSPojoDeserializer())
                    .build();

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topic1, max, topic1);
                    publishRecords(env, topic2, max, topic2);

                    env.setParallelism(splits.size());

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topic1)
                        .process(new ProcessFunction<AMPSPojo, String>() {
                            @Override
                            public void processElement(AMPSPojo value, ProcessFunction<AMPSPojo, String>.Context ctx, org.apache.flink.util.Collector<String> output) throws Exception {
                                if (value.data.equals(topic1)) {
                                    values.offer(value.data);
                                } else if (value.data.equals(topic2)) {
                                    values2.offer(value.data);
                                } else {
                                    throw new RuntimeException("Received unexpected data '" + value.data + "'");
                                }
                            }
                        });

                    env.execute(topic1);

                    assertEquals(max, values.size() + values2.size(), "All intended messages should have arrived");
                    assertTrue(values.size() == values2.size(), "Both splits should have received an equal amount of messages");
                    assertEquals(topic1, values.peek(), "Topic 1 should have intended data");
                    assertEquals(topic2, values2.peek(), "Topic 2 should have intended data");
                }
            }
        }

        @Nested
        public class QueueJobTest {
            private static LinkedBlockingQueue<String> values;
            
            @BeforeEach
            public void resetValues() {
                values = new LinkedBlockingQueue<>(100);
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testAtLeastOnceQueueJob() throws Exception {
                final String topicPublisher = PREFIX + "AtLeastOnceQueueJobWork";
                final String topicSubscriber = PREFIX + "AtLeastOnceQueueJobWorkToDo";

                final int max = 10;
                final int checkpointInterval = 250;
                final int waitForMessageInQueueTime = 500;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setClientName(topicSubscriber + "-sub")
                    .setTopic(topicSubscriber)
                    .setQueueSemantics("at-least-once")
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setOptions("max_backlog=4")
                    .setAckBatchSize(2)
                    .setAckTimeout(250)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topicPublisher, max, "a");

                    env.enableCheckpointing(checkpointInterval);

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topicSubscriber)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) {
                                values.offer(value);
                            }
                        });

                    jc = env.executeAsync(topicSubscriber);

                    List<String> messages = new ArrayList<>();
                    String message = "";
                    while (messages.size() < max && message != null) {
                        message = values.poll(waitForMessageInQueueTime, TimeUnit.MILLISECONDS);

                        if (message != null) {
                            messages.add(message);
                        }
                    }
                    assertNotNull(message, "Should not wait too long for message from queue");
                    assertEquals(max, messages.size(), "Should process intended messages");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testAtMostOnceQueueJob() throws Exception {
                final String topicPublisher = PREFIX + "AtMostOnceQueueJobWork";
                final String topicSubscriber = PREFIX + "AtMostOnceQueueJobWorkToDo";

                final int max = 10;
                final int waitForMessageInQueueTime = 250;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setClientName(topicSubscriber + "-sub")
                    .setTopic(topicSubscriber)
                    .setQueueSemantics("at-most-once")
                    .setOptions("max_backlog=4")
                    .setAckBatchSize(2)
                    .setAckTimeout(250)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                JobClient jc = null;

                try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();) {
                    publishRecords(env, topicPublisher, max, "a");

                    env.fromSource(source, WatermarkStrategy.noWatermarks(), topicSubscriber)
                        .process(new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, org.apache.flink.util.Collector<String> output) {
                                values.offer(value);
                            }
                        });

                    jc = env.executeAsync(topicSubscriber);

                    List<String> messages = new ArrayList<>();
                    String message = "";
                    while (messages.size() < max && message != null) {
                        message = values.poll(waitForMessageInQueueTime, TimeUnit.MILLISECONDS);

                        if (message != null) {
                            messages.add(message);
                        }
                    }
                    assertNotNull(message, "Should not wait too long for message from queue");
                    assertEquals(max, messages.size(), "Should process intended messages");
                } finally {
                    if (jc != null) {
                        jc.cancel().join();
                    }
                }
            }
        }

        private void publishRecords(StreamExecutionEnvironment env, String topic, long max, String data) throws Exception {
            DataGeneratorSource<AMPSPojo> generator = new AMPSPojoDataGenerator(max, data);
            
            AMPSSink<AMPSPojo> sink = AMPSSink.<AMPSPojo>builder()
                .setTopic(topic)
                .setClientName(topic + "-record-init")
                .setUri(TestConstants.URI)
                .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                .setUseSuffix(true)
                .build();

            env.fromSource(generator, WatermarkStrategy.noWatermarks(), topic)
                    .sinkTo(sink);

            env.execute(topic + "-record-init");
        }
    }

    private String getData() {
        return "d" + System.currentTimeMillis();
    }
}

