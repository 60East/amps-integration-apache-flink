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

import java.beans.ExceptionListener;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.FixedDelayStrategy;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.exception.ConnectionException;
import com.crankuptheamps.client.exception.ConnectionRefusedException;
import com.crankuptheamps.client.fields.CommandField;
import com.crankuptheamps.flink.source.AMPSSource;
import com.crankuptheamps.flink.source.metrics.SourceMetrics;
import com.crankuptheamps.flink.source.reader.AMPSRecordEmitter;
import com.crankuptheamps.flink.source.reader.AMPSSourceReader;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.testutils.AMPSMessageHandler;
import com.crankuptheamps.flink.testutils.AMPSPojo;
import com.crankuptheamps.flink.testutils.AMPSReaderOutput;
import com.crankuptheamps.flink.testutils.AMPSSourceReaderContext;
import com.crankuptheamps.flink.testutils.MessageDeserializationSchema;
import com.crankuptheamps.flink.testutils.SSLConnectorInitializer;
import com.crankuptheamps.flink.testutils.TestConstants;
import com.crankuptheamps.flink.util.AMPSSourceHeaderKeys;
import com.crankuptheamps.flink.util.AMPSMessage;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSSourceReaderTest {
    
    public static final long MAX_WAIT_MS = 500;
    public static final long READER_SHORT_WAIT_MS = 50;
    public static final long READER_LONG_WAIT_MS = 150;

    @BeforeAll
    public static void initIfAMPSRunning() {
        try {
            URL ampsUrl = new URL(TestConstants.URL);

            HttpURLConnection c = (HttpURLConnection) ampsUrl.openConnection();
            c.setRequestMethod("GET");
            c.connect();

            assertEquals(200, c.getResponseCode(), "Should have connected to test AMPS instance");
        } catch (Exception e) {
            throw new RuntimeException("Exception when connecting to AMPS", e);
        }
    }

    @Nested
    public class ReaderAndClientSynergy {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testIsAvailable() throws Exception {
            String topic = "testIsAvailable";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();

                pub.connect(TestConstants.URI);
                pub.logon();

                CompletableFuture<Void> completableFuture = reader.isAvailable();
                assertFalse(completableFuture.isDone(), "Should have incomplete future");

                reader.addSplits(splits);
                Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for client

                pub.publish(topic, "1");
                pub.publishFlush(10000L);
                
                completableFuture.get(10, TimeUnit.SECONDS);
                assertTrue(completableFuture.isDone(), "Should have completed future");
            }
        }
        
        @Nested
        public class PollNext {
    
            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPollNext() throws Exception {
                String topic = "testPollNext";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);
                    Thread.sleep(READER_SHORT_WAIT_MS);
            
                    pub.connect(TestConstants.URI);
                    pub.logon();

                    InputStatus status;

                    status = reader.pollNext(output);
                    assertEquals(InputStatus.NOTHING_AVAILABLE, status, "Should have no records");

                    pub.publish(topic, "1");
                    pub.publishFlush(10000L);

                    waitForSpecifiedInput(reader, output, 1);
                    status = reader.pollNext(output);

                    assertEquals(InputStatus.NOTHING_AVAILABLE, status, "Should have finished polling record");
                    assertEquals(1, output.getRecords().size(), "Should have one record");
                }
            }
            
            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPollNextWithSleep() throws Exception {
                String topic = "testPollNextWithSleep";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                final int sleepDuration = 100;

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setSleepMillisAfterBlock(sleepDuration)
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);
            
                    pub.connect(TestConstants.URI);
                    pub.logon();

                    long before = System.currentTimeMillis();
                    
                    pub.publish(topic, "1");

                    waitForSpecifiedInput(reader, output, 1);
                    reader.pollNext(output);

                    long after = System.currentTimeMillis();

                    assertEquals(1, output.getRecords().size(), "Should have one record");
                    // Allow the duration to be somewhat off
                    assertTrue(after - before >= sleepDuration / 3, "Should have slept a short duration");
                }
            }

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPollNextNoSplits() throws Exception {
                String topic = "testPollNextNoSplits";

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.notifyNoMoreSplits();
                    
                    InputStatus status = waitForSpecifiedInput(reader, output, 1);

                    assertEquals(InputStatus.END_OF_INPUT, status, "Should end input");
                }
            }

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPollNextSOW() throws Exception {
                String topic = "testPollNextSOW";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setSubscribeCommand("sow")
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);
                    reader.notifyNoMoreSplits();

                    pub.connect(TestConstants.URI);
                    pub.logon();

                    TestConstants.sowDelete(pub, topic);

                    pub.publish(topic, "{\"id\":1,\"data\":\"aaaa\"}");
                    pub.publishFlush(10000L);
                    InputStatus status = waitForSpecifiedInput(reader, output, 2);

                    assertEquals(InputStatus.END_OF_INPUT, status, "Should have finished SOW query");
                    for (String record : output.getRecords()) {
                        assertTrue(
                            record != null && !record.isBlank(),
                            "Message with AckType.Completed should not be emitted"); 
                    }
                }
            }

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPollNextSOWAndSubscribe() throws Exception {
                String topic = "testPollNextSOWAndSubscribe";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setSubscribeCommand("sow_and_subscribe")
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);
                    reader.notifyNoMoreSplits();

                    pub.connect(TestConstants.URI);
                    pub.logon();
                    
                    TestConstants.sowDelete(pub, topic);

                    pub.publish(topic, "{\"id\":1,\"data\":\"aaaa\"}");
                    pub.publishFlush(10000L);

                    InputStatus status = waitForSpecifiedInput(reader, output, 100, 10);

                    assertEquals(InputStatus.NOTHING_AVAILABLE, status, "Should be waiting for more input");
                    for (String record : output.getRecords()) {
                        assertTrue(
                            record != null && !record.isBlank(),
                            "Message with AckType.Completed should not be emitted"); 
                    }
                }
            }
            
            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPollNextSOWMultipleSplits() throws Exception {
                String topic = "testPollNextSOWMultipleSplits";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic, "/id MOD 2 = 0"));
                splits.add(new AMPSSplit(topic, "/id MOD 2 = 1"));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setSubscribeCommand("sow")
                    .build();

                try (Client pub = new Client(topic + "-pub");) {
                    pub.connect(TestConstants.URI);
                    pub.logon();

                    TestConstants.sowDelete(pub, topic);

                    pub.publish(topic, "{\"id\":1,\"data\":\"aaaa\"}");
                    pub.publish(topic, "{\"id\":2,\"data\":\"aaaa\"}");
                    pub.publishFlush(10000L);

                    for (int i = 0; i < 10; i++) {
                        try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                            reader.start();
                            reader.addSplits(splits);
                            reader.notifyNoMoreSplits();
                            
                            output.getRecords().clear();
                            InputStatus status = waitForSpecifiedInput(reader, output, 3);

                            assertEquals(InputStatus.END_OF_INPUT, status, "Should have finished SOW query");
                            assertEquals(2, output.getRecords().size(), "Should have received the two SOW records");
                            for (String record : output.getRecords()) {
                                assertTrue(
                                    record != null && !record.isBlank(),
                                    "Message with AckType.Completed should not be emitted"); 
                            }
                        }
                    }
                }
            }
        }

        @Nested
        public class PauseOrResume {

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPauseOrResumeSplitBookmark() throws Exception {
                String topic = "testPauseOrResumeSplitBookmark";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic));
                List<String> splitIds = new ArrayList<>();
                splitIds.add(splits.get(0).splitId());

                String data = getData();
                
                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setBookmark("0")
                    .setOptions("rate=100")
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setContentFilter("/data = '" + data + "'")
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);

                    pub.connect(TestConstants.URI);
                    pub.logon();
                    
                    for (int i = 0; i < 10; i++) {
                        pub.publish(topic, "{\"id\":" + i + ",\"data\":\"" + data + "\"}");
                    }

                    pub.publishFlush(10000L);
                    
                    reader.isAvailable().join();

                    reader.pauseOrResumeSplits(splitIds, Collections.emptyList());

                    waitForSpecifiedInput(reader, output, 100, 10);
                    int beforePauseLastId = getRecordId(output, output.getRecords().size() - 1); 

                    reader.pauseOrResumeSplits(Collections.emptyList(), splitIds);

                    waitForSpecifiedInput(reader, output, 1);
                    int afterPauseFirstId = getRecordId(output, output.getRecords().size() - 1);

                    assertEquals(afterPauseFirstId, beforePauseLastId + 1, "Should resume from most recent bookmark");
                }
            }

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testPauseOrResumeSplit() throws Exception {
                String topic = "testPauseOrResumeSplit";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic));
                List<String> splitIds = new ArrayList<>();
                for (AMPSSplit s : splits) {
                    splitIds.add(s.splitId());
                }
                splits.add(new AMPSSplit(topic));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);
                    Thread.sleep(READER_SHORT_WAIT_MS);

                    pub.connect(TestConstants.URI);
                    pub.logon();
                    
                    pub.publish(topic, "1");
                    pub.publishFlush(10000L);
                    waitForSpecifiedInput(reader, output, 2);

                    reader.pauseOrResumeSplits(splitIds, Collections.emptyList());
                    Thread.sleep(READER_LONG_WAIT_MS); // Wait briefly for the subscription

                    pub.publish(topic, "2");
                    pub.publishFlush(10000L);
                    waitForSpecifiedInput(reader, output, 1);

                    reader.pauseOrResumeSplits(Collections.emptyList(), splitIds);
                    Thread.sleep(READER_LONG_WAIT_MS); // Wait briefly for the subscription

                    pub.publish(topic, "3");
                    pub.publishFlush(10000L);
                    waitForSpecifiedInput(reader, output, 2);
                    
                    assertEquals("1", output.getRecords().get(0), "Should have received first message twice");
                    assertEquals("1", output.getRecords().get(1), "Should have received first message twice");
                    assertEquals("2", output.getRecords().get(2), "Should have received second message once");
                    assertEquals("3", output.getRecords().get(3), "Should have received third message twice");
                    assertEquals("3", output.getRecords().get(4), "Should have received third message twice");
                }
            }
        }

        @Nested
        public class AddSplits {

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testMultipleSplits() throws Exception {
                String topic = "testMultipleSplits";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic, "/id MOD 2 = 0"));
                splits.add(new AMPSSplit(topic, "/id MOD 2 = 1"));

                List<AMPSSplit> split = new ArrayList<>();
                split.add(splits.get(0));

                AMPSReaderOutput<String> outputTwoSplits = new AMPSReaderOutput<>();
                AMPSReaderOutput<String> outputOneSplit = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setUseSuffix(true)
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> readerOneSplit = source.createReader(new AMPSSourceReaderContext());
                        SourceReader<String, AMPSSplit> readerTwoSplits = source.createReader(new AMPSSourceReaderContext());) {
                    readerTwoSplits.start();
                    readerTwoSplits.addSplits(splits);
                    readerOneSplit.start();
                    readerOneSplit.addSplits(split);
                    Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for clients to set up
            
                    pub.connect(TestConstants.URI);
                    pub.logon();

                    pub.publish(topic, "{\"id\":1,\"data\":\"aaaa\"}");
                    pub.publish(topic, "{\"id\":2,\"data\":\"aaaa\"}");
                    pub.publishFlush(10000L);

                    waitForSpecifiedInput(readerTwoSplits, outputTwoSplits, 100, 2);
                    waitForSpecifiedInput(readerOneSplit, outputOneSplit, 100, 2);

                    assertEquals(2, outputTwoSplits.getRecords().size(), "Two splits should have two messages");
                    assertEquals(1, outputOneSplit.getRecords().size(), "One split should have one message");
                }
            }

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testMultipleTopicSplits() throws Exception {
                String topic1 = "testMultipleTopicSplits1";
                String topic2 = "testMultipleTopicSplits2";

                List<AMPSSplit> splits = new ArrayList<>();
                splits.add(new AMPSSplit(topic1, ""));
                splits.add(new AMPSSplit(topic2, ""));

                AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic("testMultipleTopicSplits")
                    .setClientName("testMultipleTopicSplits")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (Client c1 = new Client(topic1 + "-pub"); Client c2 = new Client(topic2 + "-pub");
                        SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    c1.connect(TestConstants.URI);
                    c1.logon();

                    c2.connect(TestConstants.URI);
                    c2.logon();

                    reader.start();
                    reader.addSplits(splits);
                    Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for subscriptions

                    c1.publish(topic1, "1");
                    c2.publish(topic2, "2");
                    
                    waitForSpecifiedInput(reader, output, 2);

                    assertTrue(output.getRecords().contains("1"), "Should have message from first topic.");
                    assertTrue(output.getRecords().contains("2"), "Should have message from second topic.");
                }
            }

            @Test
            @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
            public void testFilterWithSplitFilter() throws Exception {
                String topic = "testFilterWithSplitFilter";

                List<AMPSSplit> filterSplit = new ArrayList<>();
                filterSplit.add(new AMPSSplit(topic, "/id MOD 2 = 0"));

                List<AMPSSplit> noFilterSplit = new ArrayList<>();
                noFilterSplit.add(new AMPSSplit(topic));

                AMPSReaderOutput<String> outputFilterSplit = new AMPSReaderOutput<>();
                AMPSReaderOutput<String> outputNoFilterSplit = new AMPSReaderOutput<>();

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setContentFilter("/id > 2")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .setUseSuffix(true)
                    .build();

                try (Client pub = new Client(topic + "-pub");
                        SourceReader<String, AMPSSplit> readerFilterSplit = source.createReader(new AMPSSourceReaderContext());
                        SourceReader<String, AMPSSplit> readerNoFilterSplit = source.createReader(new AMPSSourceReaderContext());) {
                    readerFilterSplit.start();
                    readerFilterSplit.addSplits(filterSplit);
                    readerNoFilterSplit.start();
                    readerNoFilterSplit.addSplits(noFilterSplit);

                    pub.connect(TestConstants.URI);
                    pub.logon();

                    pub.publish(topic, "{\"id\":1,\"data\":\"aaaa\"}");
                    pub.publish(topic, "{\"id\":2,\"data\":\"aaaa\"}");
                    pub.publish(topic, "{\"id\":3,\"data\":\"aaaa\"}");
                    pub.publish(topic, "{\"id\":4,\"data\":\"aaaa\"}");
                    pub.publishFlush(10000L);

                    waitForSpecifiedInput(readerFilterSplit, outputFilterSplit, 100, 2);
                    waitForSpecifiedInput(readerNoFilterSplit, outputNoFilterSplit, 100, 3);

                    assertEquals(InputStatus.NOTHING_AVAILABLE, readerFilterSplit.pollNext(outputFilterSplit), "Should have read intended messages");
                    assertEquals(InputStatus.NOTHING_AVAILABLE, readerNoFilterSplit.pollNext(outputNoFilterSplit), "Should have read intended messages");
                    assertEquals(2, outputNoFilterSplit.getRecords().size(), "Should have intended message count");
                    assertEquals(1, outputFilterSplit.getRecords().size(), "Should have intended message count");
                }
            } 
        }
    }

    @Nested
    public class BookmarkSubscriptions {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testBookmarkSubscriptionWithTimestamp() throws Exception {
            String topic = "testBookmarkSubscriptionWithTimestamp";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            final int max = 3;

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setBookmark("0")
                .setOptions("timestamp,")
                .setTopN(max)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                for (int i = 0; i < max; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);

                waitForSpecifiedInput(reader, output, max);

                assertEquals(max, output.getRecords().size(), "Should have records.");
                assertEquals(max, output.getTimestamps().size(), "Should have timestamps.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testTopNBookmarkSubscription() throws Exception {
            String topic = "testTopNBookmarkSubscription";
            final int topN = 1;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setBookmark("0")
                .setTopN(topN)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                for (int i = 0; i < topN; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);
                reader.notifyNoMoreSplits();

                InputStatus status = waitForSpecifiedInput(reader, output, topN + 1);
                
                assertEquals(InputStatus.END_OF_INPUT, status, "Should be end of input");
                assertEquals(topN, output.getRecords().size(), "Should have topN records");
                for (String record : output.getRecords()) {
                    assertTrue(
                        record != null && !record.isBlank(),
                        "Message with AckType.Completed should not be emitted"); 
                }
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testCheckpointBookmarkSubscription() throws Exception {
            String topic = "testCheckpointBookmarkSubscription";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic, "", "0"));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setBookmark("0")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                for (int i = 0; i < 3; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);

                AMPSSplit firstSnapshot = reader.snapshotState(1).get(0);
                reader.notifyCheckpointComplete(1);
                assertEquals("0", firstSnapshot.getBookmark(), "First snapshot should match first bookmark from source");

                AMPSSplit secondSnapshot = reader.snapshotState(2).get(0); 
                reader.notifyCheckpointComplete(2);
                assertEquals(
                    firstSnapshot.getBookmark(),
                    secondSnapshot.getBookmark(),
                    "Should have same bookmark since no input was read yet");


                waitForSpecifiedInput(reader, output, 1);
                AMPSSplit thirdSnapshot = reader.snapshotState(3).get(0);
                reader.notifyCheckpointComplete(3);
                assertNotEquals(
                    firstSnapshot.getBookmark(),
                    thirdSnapshot.getBookmark(),
                    "Should have different bookmark since input was read and a snapshot was taken");
            }
        }
    
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testBookmarkRangeSplits() throws Exception {
            String topic = "testBookmarkRangeSplits";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic, "", "[20200604T000000:20200605T000000)"));
            splits.add(new AMPSSplit(topic, "", "[20200604T000000:20200605T000000]"));
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .build();

            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(splits);
                reader.notifyNoMoreSplits();

                assertEquals(InputStatus.END_OF_INPUT, waitForSpecifiedInput(reader, output, 1), "Should be bounded.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSetsBookmarkWhenNoRecordsEmitted() throws Exception {
            String topic = "testSetsBookmarkWhenNoRecordsEmitted";

            AMPSSplit split = new AMPSSplit(topic, "", "0");

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(split);

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setBookmark("")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(splits);

                List<AMPSSplit> snapshottedSplits = reader.snapshotState(1L);

                assertEquals(1, snapshottedSplits.size(), "Should have one split");

                AMPSSplit snapshottedSplit = snapshottedSplits.get(0);

                assertEquals(split.getTopic(), snapshottedSplit.getTopic(), "Should have same topic");
                assertEquals(split.getSplitFilter(), snapshottedSplit.getSplitFilter(), "Should have same filter");
                assertEquals(split.getBookmark(), snapshottedSplit.getBookmark(), "Should have same bookmark");
            }
        }
    }

    @Nested
    public class SOW {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testDeltaSubscribe() throws Exception {
            String topic = "testDeltaSubscribe";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<AMPSPojo> output = new AMPSReaderOutput<>();

            AMPSSource<AMPSPojo> source = AMPSSource.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getAMPSPojoDeserializer())
                .setSubscribeCommand("delta_subscribe")
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<AMPSPojo, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                TestConstants.sowDelete(pub, topic);

                SerializationSchema<AMPSPojo> serSchema = new JsonSerializationSchema<>();
                serSchema.open(null);

                AMPSPojo pojo1 = new AMPSPojo(1, "1");
                byte[] pojoBytes = serSchema.serialize(pojo1);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                AMPSPojo pojo2 = new AMPSPojo(2, "1");
                pojoBytes = serSchema.serialize(pojo2);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);
                Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for subscription

                pojoBytes = serSchema.serialize(pojo1);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                pojo2.data = "2";
                pojoBytes = serSchema.serialize(pojo2);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                pub.publishFlush(10000L);

                waitForSpecifiedInput(reader, output, 2);
                
                boolean found1 = false;
                boolean found2 = false;

                for (AMPSPojo record : output.getRecords()) {
                    if (record.id == pojo1.id) {
                        AMPSPojo defaultData = new AMPSPojo();
                        assertEquals(defaultData.data, record.data, "Should have the default data for fields not updated by the delta subscribe.");
                        assertEquals(defaultData.num, record.num, "Should have the default data for fields not updated by the delta subscribe.");
                        found1 = true;
                    } else if (record.id == pojo2.id) {
                        AMPSPojo defaultData = new AMPSPojo();
                        assertEquals(pojo2.data, record.data, "Should have the updated data for fields updated by the delta subscribe.");
                        assertEquals(defaultData.num, record.num, "Should have the default data for fields not updated by the delta subscribe.");
                        found2 = true;
                    }
                }

                assertTrue(found1, "Should have received message from the message that did not change.");
                assertTrue(found2, "Should have reveived message from the message that did change.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWAndDeltaSubscribe() throws Exception {
            String topic = "testSOWAndDeltaSubscribe";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<AMPSPojo> output = new AMPSReaderOutput<>();

            AMPSSource<AMPSPojo> source = AMPSSource.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getAMPSPojoDeserializer())
                .setSubscribeCommand("sow_and_delta_subscribe")
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<AMPSPojo, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);

                SerializationSchema<AMPSPojo> serSchema = new JsonSerializationSchema<>();
                serSchema.open(null);

                AMPSPojo pojo1 = new AMPSPojo(1, "1", 1);
                byte[] pojoBytes = serSchema.serialize(pojo1);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                AMPSPojo pojo2 = new AMPSPojo(2, "1", 1);
                pojoBytes = serSchema.serialize(pojo2);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);

                waitForSpecifiedInput(reader, output, 2);
                
                AMPSPojo pojo1Updated = new AMPSPojo(pojo1.id, pojo1.data, 100);
                pojoBytes = serSchema.serialize(pojo1Updated);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                AMPSPojo pojo2Updated = new AMPSPojo(pojo2.id, "2", pojo2.num);
                pojoBytes = serSchema.serialize(pojo2Updated);
                pub.publish(topic.getBytes(), 0, topic.getBytes().length, pojoBytes, 0, pojoBytes.length);
                pub.publishFlush(10000L);

                waitForSpecifiedInput(reader, output, 2);
                
                boolean found1 = false;
                boolean found1Updated = false;
                boolean found2 = false;
                boolean found2Updated = false;

                for (AMPSPojo record : output.getRecords()) {
                    if (record.id == pojo1.id) {
                        if (!found1) {
                            assertEquals(pojo1, record, "Both pojo1s should be equal.");
                            found1 = true;
                        } else {
                            AMPSPojo defaultData = new AMPSPojo();
                            assertEquals(defaultData.data, record.data, "Should have the default data for fields not updated by the delta subscribe.");
                            assertEquals(pojo1Updated.num, record.num, "Should have the updated data for fields not updated by the delta subscribe.");
                            found1Updated = true;
                        }
                    } else if (record.id == pojo2.id) {
                        if (!found2) {
                            assertEquals(pojo2, record, "Both pojo2s should be equal.");
                            found2 = true;
                        } else {
                            AMPSPojo defaultData = new AMPSPojo();
                            assertEquals(defaultData.num, record.num, "Should have the default data for fields not updated by the delta subscribe.");
                            assertEquals(pojo2Updated.data, record.data, "Should have the updated data for fields not updated by the delta subscribe.");
                            found2Updated = true;
                        }
                    }
                }

                assertTrue(found1 && found1Updated && found2 && found2Updated, "Should have received message all intended messages.");
            }
        }
    
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testTopNSOW() throws Exception {
            String topic = "testTopNSOW";
            final int topN = 3;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setTopN(topN)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setSubscribeCommand("sow")
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);

                for (int i = 0; i < topN + 1; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);
                reader.notifyNoMoreSplits();

                InputStatus status = waitForSpecifiedInput(reader, output, topN + 1);

                assertEquals(InputStatus.END_OF_INPUT, status, "Should be end of input");
                assertEquals(topN, output.getRecords().size(), "Should have received topN records");
                for (String record : output.getRecords()) {
                    assertTrue(
                        record != null && !record.isBlank(),
                        "Message with AckType.Completed should not be emitted"); 
                }
            }
        }
    
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSkipNSOW() throws Exception {
            String topic = "testSkipNSOW";
            final int skipN = 1;
            final int topN = 1;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setTopN(topN)
                .setSkipN(skipN)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setSubscribeCommand("sow")
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);

                for (int i = 0; i < skipN + topN; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);
                reader.notifyNoMoreSplits();

                waitForSpecifiedInput(reader, output, topN);

                assertEquals(topN, output.getRecords().size(), "Should have topN records");

                for (int i = skipN; i < skipN + topN; i++) {
                    // Since skipN records were skipped, offset the index of the list with skipN to compare the id
                    assertEquals(i, getRecordId(output, i - skipN), "Should have equal id field starting at id = skipN");
                }
            }
        }
    
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSkipNOptions() throws Exception {
            String topic = "testSkipNOptions";
            final int skipN = 1;
            final int topN = 1;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setTopN(topN)
                .setSkipN(skipN)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setSubscribeCommand("sow")
                .setOptions("timestamp")
                .build();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);

                for (int i = 0; i < skipN + topN; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);
                reader.notifyNoMoreSplits();

                waitForSpecifiedInput(reader, output, topN);

                assertEquals(topN, output.getRecords().size(), "Should have topN records");
                assertTrue(1 <= output.getTimestamps().size(), "Should have timestamps because of timestamp option");

                for (int i = skipN; i < skipN + topN; i++) {
                    // Since skipN records were skipped, offset the index of the list with skipN to compare the id
                    assertEquals(i, getRecordId(output, i - skipN), "Should have equal id field starting at id = skipN");
                }
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWOrderBy() throws Exception {
            String topic = "testSOWOrderBy";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setOrderBy("/data DESC")
                .setSubscribeCommand("sow")
                .build();
            
            AMPSSourceReaderContext context = new AMPSSourceReaderContext();

            try (Client pub = new Client(topic + "-pub"); 
                    SourceReader<String, AMPSSplit> reader = source.createReader(context);) {
                pub.connect(TestConstants.URI);
                pub.logon();

                TestConstants.sowDelete(pub, topic);

                pub.publish(topic, "{\"id\":1,\"data\":\"a\"}");
                pub.publish(topic, "{\"id\":2,\"data\":\"b\"}");
                pub.publish(topic, "{\"id\":3,\"data\":\"c\"}");
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);

                waitForSpecifiedInput(reader, output, 3);
                int lastRecordId = getRecordId(output, 0);
                int currentRecordId = -1;

                for (int i = 1; i < output.getRecords().size(); i++) {
                    currentRecordId = getRecordId(output, i);
                    assertEquals(lastRecordId, currentRecordId + 1, "Should be descending.");
                    lastRecordId = currentRecordId;
                }
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWBatchSize() throws Exception {
            String topic = "testSOWBatchSize";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            final int batchSize = 2;

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setBatchSize(batchSize)
                .setSubscribeCommand("sow")
                .build();

            class DummyClient extends HAClient {
                
                public Command command;

                @Override
                public CommandId executeAsync(Command command, MessageHandler handler) {
                    this.command = command;
                    return null;
                }
            }
            
            try (SourceReader<String, AMPSSplit> reader = source.createReaderWithClient(new AMPSSourceReaderContext(), new DummyClient());) {
                DummyClient readerClient = (DummyClient) ((AMPSSourceReader<String>) reader).getClient();
                
                reader.start();
                reader.addSplits(splits);

                int maxLoops = 100;
                int loops = 0;

                while (readerClient.command == null && loops < maxLoops) {
                    Thread.sleep(10);
                    loops++;
                }

                assertTrue(loops < maxLoops, "Should not wait too long for reader to execute the command");
                assertNotNull(readerClient.command, "Should have a non null command");
                assertEquals(batchSize, readerClient.command.getBatchSize(), "Should have set batch size");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWAndSubscribe() throws Exception {
            String topic = "testSOWAndSubscribe";
            
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setSubscribeCommand("sow_and_subscribe")
                .build();

            try (Client pub = new Client(topic + "-pub"); 
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);

                pub.publish(topic, "{\"id\":1}");
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);

                waitForSpecifiedInput(reader, output, 1);
                
                pub.publish(topic, "{\"id\":2}");
                pub.publishFlush(10000L);

                waitForSpecifiedInput(reader, output, 1);
                
                assertEquals(2, output.getRecords().size(), "Should have received all intended messages");
                for (String str : output.getRecords()) {
                    assertFalse(str.isBlank(), "Should not be a blank message");        
                }
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWAndSubscribeTopNBookmarkSubscription() throws Exception {
            String topic = "testSOWAndSubscribeTopNBookmarkSubscription";

            final int topN = 2;
            final int pubAmount = topN * 2;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();


            try (Client pub = new Client(topic + "-pub");
                    ) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);

                for (int i = 0; i < pubAmount; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"aaaa\"}");
                }
                pub.publishFlush(10000L);

                AMPSMessageHandler mh = new AMPSMessageHandler();
                pub.sow(mh, topic, "", "", "", 10, 1, "timestamp", 0);
                String ts = mh.timestamps.poll(1000, TimeUnit.MILLISECONDS);

                assertNotNull(ts, "Should have a timestamp to use as a bookmark");

                AMPSSource<String> source = AMPSSource.<String>builder()
                    .setUri(TestConstants.URI)
                    .setTopic(topic)
                    .setClientName(topic)
                    .setBookmark(ts)
                    .setTopN(topN)
                    .setSubscribeCommand("sow_and_subscribe")
                    .setDeserializationSchema(TestConstants.getStringSchema())
                    .build();

                try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                    reader.start();
                    reader.addSplits(splits);
                    reader.notifyNoMoreSplits();

                    InputStatus status = waitForSpecifiedInput(reader, output, pubAmount);
                    
                    assertNotEquals(InputStatus.END_OF_INPUT, status, "Should not be end of input");
                    assertNotEquals(0, output.getRecords().size(), "Should have records from sow query and subscription");
                    for (String record : output.getRecords()) {
                        assertTrue(
                            record != null && !record.isBlank(),
                            "Message with AckType.Completed should not be emitted"); 
                    }
                }
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWAndSubscribeTopN() throws Exception {
            topNSOWAndSubscribe("sow_and_subscribe", "testSOWAndSubscribeTopN");
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWAndDeltaSubscribeTopN() throws Exception {
            topNSOWAndSubscribe("sow_and_delta_subscribe", "testSOWAndDeltaSubscribeTopN");
        }

        private void topNSOWAndSubscribe(String command, String topic) throws Exception {
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setSubscribeCommand(command)
                .setTopN(1)
                .build();

            try (Client pub = new Client(topic + "-pub"); 
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                TestConstants.sowDelete(pub, topic);

                String first = "{\"id\":1}";
                String second = "{\"id\":2}";
                String third = "{\"id\":3}";

                pub.publish(topic, first);
                pub.publish(topic, second);
                pub.publishFlush(10000L);

                reader.start();
                reader.addSplits(splits);

                waitForSpecifiedInput(reader, output, 1);
                
                pub.publish(topic, third);
                pub.publishFlush(10000L);

                waitForSpecifiedInput(reader, output, 1);
                
                List<String> records = output.getRecords();
                assertEquals(2, records.size(), "Should have received all intended messages");
                assertTrue(records.get(0).equals(first) || records.get(0).equals(second),
                    "Should have received first or second message from the SOW topN query");
                assertEquals(third, records.get(1),
                    "Should have received third message after skipping a message with SOW topN");
            }
        }
    }

    @Nested
    public class ClientSetUp {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testHeartbeatWithInterval() throws Exception {
            String topic = "testHeartbeatWithInterval";
    
            final int intervalSeconds = 5;

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setHeartbeat(intervalSeconds)
                .build();

            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));
                
                HAClient client = ((AMPSSourceReader<String>) reader).getClient();

                assertEquals(intervalSeconds, client.getHeartbeatInterval(), "Should be same interval.");
                assertEquals(intervalSeconds * 2, client.getReadTimeout(), "Should be double the interval.");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testExceptionListener() throws Exception {
            String topic = "testExceptionListener";

            class EL implements ExceptionListener {
                public static final LinkedBlockingQueue<Exception> exs = new LinkedBlockingQueue<>(5);

                @Override
                public void exceptionThrown(Exception e) {
                    exs.offer(e);
                }
            }

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setExceptionListenerSupplier(() -> new EL())
                .build();

            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));

                HAClient client = ((AMPSSourceReader<String>) reader).getClient();
            
                assertInstanceOf(EL.class, client.getExceptionListener(), "Should have set the exception listener");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceReconnectDelayStrategy() throws Exception {
            String topic = "testSourceReconnectDelayStrategy";

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setReconnectDelayStrategySupplier(() -> new FixedDelayStrategy(100, 100))
                .build();
            
            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));
                Thread.sleep(READER_SHORT_WAIT_MS);
                
                HAClient client = ((AMPSSourceReader<String>) reader).getClient();

                assertInstanceOf(FixedDelayStrategy.class, client.getReconnectDelayStrategy(), "Should have set reconnect delay strategy");
            }
        }
        
        @Test
        @ResourceLock(value = "DISABLE_SOURCE_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceServerChooser() throws Exception {
            String topic = "testSourceServerChooser";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            String data = getData();

            final int messagesToPublish = 10;
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setServerChooserSupplier(() -> {
                    DefaultServerChooser chooser = new DefaultServerChooser();
                    chooser.add(TestConstants.SOURCE_DISABLE_URI);
                    chooser.add(TestConstants.URI);
                    return chooser;
                })
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setBookmark("0")
                .setContentFilter("/data = '" + data + "'")
                .setOptions("rate=" + (messagesToPublish * 5))
                .build();

            enableSourceTransport(topic);

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                for (int i = 0; i < messagesToPublish; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"" + data + "\"}");
                }
                pub.publishFlush(10000L);
                
                reader.start();
                reader.addSplits(splits);

                reader.isAvailable().join();

                pub.publish("disableSourceServerChooserTransport", "");
                pub.publishFlush(10000L);

                waitForSpecifiedInput(reader, output, 1000, messagesToPublish);

                assertEquals(messagesToPublish, output.getRecords().size(), "All messages should have arrived with no duplicates");
            }
        }

        @Test
        @ResourceLock(value = "DISABLE_SOURCE_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceServerChooserOneUriShouldNotGetAllMessages() throws Exception {
            String topic = "testSourceServerChooser";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            String data = getData();

            final int messagesToPublish = 10;
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setServerChooserSupplier(() -> {
                    DefaultServerChooser chooser = new DefaultServerChooser();
                    chooser.add(TestConstants.SOURCE_DISABLE_URI);
                    return chooser;
                })
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setBookmark("0")
                .setContentFilter("/data = '" + data + "'")
                .setOptions("rate=100")
                .build();

            enableSourceTransport(topic);

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                for (int i = 0; i < messagesToPublish; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"" + data + "\"}");
                }
                pub.publishFlush(10000L);
                
                reader.start();
                reader.addSplits(splits);

                reader.isAvailable().join();

                pub.publish("disableSourceServerChooserTransport", "");
                pub.publishFlush(10000L);

                waitForSpecifiedInput(reader, output, 100, messagesToPublish);

                assertTrue(messagesToPublish > output.getRecords().size(), "Only some messages should have arrived");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testUseSuffixTrue() throws Exception {
            String topic = "testUseSuffixTrue";

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setUseSuffix(true)
                .build();
            
            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));
                
                HAClient client = ((AMPSSourceReader<String>) reader).getClient();

                assertTrue(client.getName().contains(topic), "Should contain the client name");
                assertNotEquals(client.getName(), topic, "Should not be equal due to the suffix");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testUseSuffixFalse() throws Exception {
            String topic = "testUseSuffixFalse";

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setUseSuffix(false)
                .build();
            
            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));
                
                HAClient client = ((AMPSSourceReader<String>) reader).getClient();

                assertEquals(topic, client.getName(), "Should have the same client name");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testUseSuffixParallelism() throws Exception {
            String topic = "testUseSuffixParallelism";

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setSplits(List.of("", ""))
                .build();
            
            try (SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext(2));) {
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));
                
                HAClient client = ((AMPSSourceReader<String>) reader).getClient();

                assertTrue(client.getName().contains(topic), "Should contain the client name");
                assertNotEquals(client.getName(), topic, "Should not be equal due to the suffix");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceSSLConnectorInitializer() throws Exception {
            String topic = "testSourceSSLConnectorInitializer";
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.S_URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setConnectorInitializer(new SSLConnectorInitializer())
                .setReconnectDelayStrategySupplier(() -> new FixedDelayStrategy(50, 4)) // Client will hang without this
                .build();
            
            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                reader.start();
                reader.addSplits(List.of(new AMPSSplit(topic)));
                Thread.sleep(READER_SHORT_WAIT_MS);
                
                pub.publish(topic, "1");
                
                waitForSpecifiedInput(reader, output, 1);
                assertEquals(1, output.getRecords().size(), "Should have received message from SSL connection");
            } catch (Exception e) {
                Throwable cause = e.getCause();

                if (cause instanceof ConnectionException) {
                    throw new RuntimeException("Could not connect using SSL transport. Ensure trust store and key store " +
                        "in src/test/resources/ssl/ are set up properly");
                } else {
                    throw e;
                }
            }
        }
    }

    @Nested
    public class MessageQueue {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testAtLeastOnceQueue() throws Exception {
            String pubTopic = "testAtLeastOnceQueueWork";
            String subTopic = "testAtLeastOnceQueueWorkToDo";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(subTopic, ""));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(subTopic)
                .setClientName(subTopic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setQueueSemantics("at-least-once")
                .build();

            try (Client pub = new Client(pubTopic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                reader.start();
                reader.addSplits(splits);

                pub.publish(pubTopic, "1");
                pub.publish(pubTopic, "2");

                waitForSpecifiedInput(reader, output, 1);
                assertEquals(1, output.getRecords().size(), "Should have one message before acking");

                reader.snapshotState(1);
                reader.notifyCheckpointComplete(1);

                waitForSpecifiedInput(reader, output, 1);
                assertEquals(2, output.getRecords().size(), "Should have two messages after first ack");

                reader.snapshotState(2);
                reader.notifyCheckpointComplete(2);
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testNackOnCheckpointAbortedQueue() throws Exception {
            String pubTopic = "testNackOnCheckpointAbortedQueueWork";
            String subTopic = "testNackOnCheckpointAbortedQueueWorkToDo";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(subTopic, ""));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(subTopic)
                .setClientName(subTopic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setQueueSemantics("at-least-once")
                .build();

            try (Client pub = new Client(pubTopic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                reader.start();
                reader.addSplits(splits);

                pub.publish(pubTopic, "1");
                pub.publish(pubTopic, "2");

                waitForSpecifiedInput(reader, output, 1);
                assertEquals(1, output.getRecords().size(), "Should have first message from queue");

                reader.snapshotState(1);
                reader.notifyCheckpointAborted(1); // Nack the emitted message since the checkpoint was aborted

                waitForSpecifiedInput(reader, output, 1);
                assertEquals(2, output.getRecords().size(), "Should have second message after nacking the first");

                reader.snapshotState(2);
                reader.notifyCheckpointComplete(2);

                // Receive the nacked message
                waitForSpecifiedInput(reader, output, 1);
                assertEquals(3, output.getRecords().size(), "Should have received an extra message due to the nack");
                
                reader.snapshotState(3);
                reader.notifyCheckpointComplete(3);
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testAtMostOnceQueue() throws Exception {
            String pubTopic = "testAtMostOnceQueueWork";
            String subTopic = "testAtMostOnceQueueWorkToDo";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(subTopic, ""));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(subTopic)
                .setClientName(subTopic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setQueueSemantics("at-most-once")
                .build();

            try (Client pub = new Client(pubTopic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                reader.start();
                reader.addSplits(splits);

                pub.publish(pubTopic, "1");
                pub.publish(pubTopic, "2");

                waitForSpecifiedInput(reader, output, 2);
                assertEquals(2, output.getRecords().size(), "Should have both messages without acking from a Flink checkpoint");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testMaxBacklog() throws Exception {
            String pubTopic = "testMaxBacklogWork";
            String subTopic = "testMaxBacklogWorkToDo";

            final int maxBacklog = 3;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(subTopic, ""));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(subTopic)
                .setClientName(subTopic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setQueueSemantics("at-least-once")
                .setMaxBacklog(maxBacklog)
                .build();

            try (Client pub = new Client(pubTopic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                reader.start();
                reader.addSplits(splits);

                for (int i = 0; i < maxBacklog; i++) {
                    pub.publish(pubTopic, "m");
                }

                waitForSpecifiedInput(reader, output, maxBacklog);
                assertEquals(maxBacklog, output.getRecords().size(), "Should have maxBacklog messages");

                reader.snapshotState(1);
                reader.notifyCheckpointComplete(1);
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testMaxBacklogWithOptions() throws Exception {
            String pubTopic = "testMaxBacklogWithOptionsWork";
            String subTopic = "testMaxBacklogWithOptionsWorkToDo";

            final int maxBacklog = 3;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(subTopic, ""));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(subTopic)
                .setClientName(subTopic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setQueueSemantics("at-least-once")
                .setMaxBacklog(maxBacklog)
                .setOptions("timestamp")
                .build();

            try (Client pub = new Client(pubTopic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();

                reader.start();
                reader.addSplits(splits);

                for (int i = 0; i < maxBacklog; i++) {
                    pub.publish(pubTopic, "m");
                }

                waitForSpecifiedInput(reader, output, maxBacklog);
                assertEquals(maxBacklog, output.getRecords().size(), "Should have maxBacklog messages");
                assertEquals(maxBacklog, output.getTimestamps().size(), "Should have maxBacklog timestamps");

                reader.snapshotState(1);
                reader.notifyCheckpointComplete(1);
            }
        }
    }

    @Nested
    public class Metrics {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        @SuppressWarnings("unchecked")
        public void testInternalBufferSize() throws Exception {
            String topic = "testInternalBufferSize";
            
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            final int internalBufferSize = 10;

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setInternalBufferSize(internalBufferSize)
                .build();
            
            AMPSSourceReaderContext context = new AMPSSourceReaderContext();
            
            try (SourceReader<String, AMPSSplit> reader = source.createReader(context);) {
                reader.start();
                reader.addSplits(splits);
                Thread.sleep(READER_SHORT_WAIT_MS);

                Metric queueSize = context.metricGroup.getMetric(SourceMetrics.Metric.REMAINING_QUEUE_CAPACITY);
                assertNotNull(queueSize, "Should have found metric.");
                assertEquals(MetricType.GAUGE, queueSize.getMetricType(), "Should be a gauge metric.");
                assertEquals(internalBufferSize, ((Gauge<Integer>) queueSize).getValue(), "Should have correct buffer size.");
            }
        }
        
        @Test
        @ResourceLock(value = "DISABLE_SOURCE_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceConnectsAndDisconnects() throws Exception {
            String topic = "testSourceConnectsAndDisconnects";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            final int internalBufferSize = 10;

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.SOURCE_DISABLE_URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setInternalBufferSize(internalBufferSize)
                .build();
            
            AMPSSourceReaderContext context = new AMPSSourceReaderContext();

            enableSourceTransport(topic);
            
            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(context);) {
                reader.start();
                reader.addSplits(splits);
                Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for client to set up

                pub.connect(TestConstants.URI);
                pub.logon();
                
                Metric connects = context.metricGroup.getMetric(SourceMetrics.Metric.CONNECTS);
                Metric disconnects = context.metricGroup.getMetric(SourceMetrics.Metric.DISCONNECTS);
                
                while (((Counter) connects).getCount() == 0) {
                    Thread.sleep(20);
                }
                assertEquals(1, ((Counter) connects).getCount(), "Should have connected.");
                assertEquals(0, ((Counter) disconnects).getCount(), "Should not have disconnected.");
                
                pub.publish("disableSourceServerChooserTransport", "");
                pub.publishFlush(10000L);
                
                while (((Counter) disconnects).getCount() == 0) {
                    Thread.sleep(20);
                }
                assertEquals(1, ((Counter) connects).getCount(), "Should only have one connect.");
                assertTrue(1 <= ((Counter) disconnects).getCount(), "Should have disconnected.");

                pub.publish("enableSourceServerChooserTransport", "");
                pub.publishFlush(10000L);

                while (((Counter) connects).getCount() == 1) {
                    Thread.sleep(20);
                }
                assertEquals(2, ((Counter) connects).getCount(), "Should have two connects.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testRecordsFromAMPS() throws Exception {
            String topic = "testRecordsFromAMPS";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .build();
            
            AMPSSourceReaderContext context = new AMPSSourceReaderContext();

            try (Client pub = new Client(topic + "-pub");
                    SourceReader<String, AMPSSplit> reader = source.createReader(context);) {
                pub.connect(TestConstants.URI);
                pub.logon();

                reader.start();
                reader.addSplits(splits);
                Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait to let reader set up
                
                Counter records = context.metricGroup.getNumRecordsInCounter();
                assertEquals(0, records.getCount(), "Should have correct records count.");

                Counter bytes = context.metricGroup.getNumBytesInCounter();
                assertEquals(0, bytes.getCount(), "Should have correct bytes count.");
                
                String data = "123";

                pub.publish(topic, data);

                waitForSpecifiedInput(reader, new AMPSReaderOutput<String>(), 1);
                assertEquals(1, records.getCount(), "Should have incremented records counter.");
                assertEquals(data.getBytes().length, bytes.getCount(), "Should have increased bytes counter.");
            }
        }
    }

    @Nested
    public class HeaderKeys {
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyCOMMAND() throws Exception {
            String topic = "testSourceHeaderKeyCOMMAND";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.COMMAND;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;

            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);

            assertEquals(messagesToWaitFor, output.getRecords().size(), "Should have received all intended messages");

            for (AMPSMessage message : output.getRecords()) {
                assertEquals(CommandField.encodeCommand(Message.Command.Publish), message.getHeader(headerKey),
                    "Should have the same string for COMMAND");
                assertEquals(Message.Command.Publish, message.getCommand(),
                    "Should have the same int field set for the command");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyTOPIC() throws Exception {
            String topic = "testSourceHeaderKeyTOPIC";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.TOPIC;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;

            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);

            for (AMPSMessage message : output.getRecords()) {
                assertEquals(topic, message.getHeader(headerKey),
                    "Should have the same string for TOPIC");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeySOW_KEY() throws Exception {
            String topic = "testSourceHeaderKeySOW_KEY";
            String bookmark = "";
            String command = "sow";
            String options = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.SOW_KEY;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish + 2; // Include group begin and group end

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));
            
            String data = getData();

            AMPSReaderOutput<AMPSMessage> output = new AMPSReaderOutput<>();

            AMPSSource<AMPSMessage> source = AMPSSource.<AMPSMessage>builder()
                .setUri(TestConstants.URI) 
                .setTopic(topic)
                .setClientName(topic)
                .setBookmark(bookmark)
                .setDeserializationSchema(new MessageDeserializationSchema())
                .setContentFilter("/data = '" + data + "'")
                .setSubscribeCommand(command)
                .setHeaderKeys(Set.of(headerKey))
                .setOptions(options)
                .build();
            
            try (Client pub = new Client(topic + "-pub");
                    SourceReader<AMPSMessage, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                TestConstants.sowDelete(pub, topic);
                
                for (int i = 0; i < messagesToPublish; i++) {
                    pub.publish(topic, "{\"id\":" + i + ",\"data\":\"" + data + "\"}");
                }
                pub.publishFlush(1000L);

                reader.start();
                reader.addSplits(splits);

                waitForSpecifiedInput(reader, output, messagesToWaitFor);
            }

            List<AMPSMessage> records = output.getRecords();

            assertEquals(messagesToWaitFor, records.size(), "Should have received all intended messages");
            assertEquals(Message.Command.GroupBegin, records.get(0).getCommand(), "Should have group begin");
            assertEquals(Message.Command.SOW, records.get(1).getCommand(), "Should have sow");
            assertEquals(Message.Command.GroupEnd, records.get(2).getCommand(), "Should have group end");
        
            assertNotNull(records.get(1).getHeader(AMPSSourceHeaderKeys.SOW_KEY), "Should have set SOW_KEY header");
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyTIMESTAMPWithOption() throws Exception {
            String topic = "testSourceHeaderKeyTIMESTAMPWithOption";
            String bookmark = "";
            String command = "subscribe";
            String options = "timestamp";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.TIMESTAMP;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;

            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);

            assertEquals(messagesToWaitFor, output.getTimestamps().size(), "Should have emitted all intended timestamps");

            for (AMPSMessage message : output.getRecords()) {
                assertNotNull(message.getHeader(headerKey), "Should have set TIMESTAMP header");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyTIMESTAMPWithoutOption() throws Exception {
            String topic = "testSourceHeaderKeyTIMESTAMPWithoutOption";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.TIMESTAMP;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;

            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);

            assertEquals(0, output.getTimestamps().size(), "Should have emitted no timestamps");

            for (AMPSMessage message : output.getRecords()) {
                assertNotNull(message.getHeader(headerKey), "Should have set TIMESTAMP header");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyBOOKMARK() throws Exception {
            String topic = "testSourceHeaderKeyBOOKMARK";
            String bookmark = "0";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.BOOKMARK;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;

            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);

            for (AMPSMessage message : output.getRecords()) {
                assertNotNull(message.getHeader(headerKey), "Should have set BOOKMARK header");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyCORRELATION_ID() throws Exception {
            String topic = "testSourceHeaderKeyCORRELATION_ID";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "cId";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.CORRELATION_ID;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;
            
            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);
            
            for (AMPSMessage message : output.getRecords()) {
                assertNotNull(message.getHeader(headerKey), "Should have set CORRELATION_ID header");
                assertEquals(correlationId, message.getHeader(headerKey), "Should have same correlation ID");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeySUBSCRIPTION_ID() throws Exception {
            String topic = "testSourceHeaderKeySUBSCRIPTION_ID";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.SUBSCRIPTION_ID;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;
            
            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);
            
            for (AMPSMessage message : output.getRecords()) {
                assertNotNull(message.getHeader(headerKey), "Should have set SUBSCRIPTION_ID header");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyLENGTH() throws Exception {
            String topic = "testSourceHeaderKeyLENGTH";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.LENGTH;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;
            
            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);
            
            for (AMPSMessage message : output.getRecords()) {
                assertNotNull(message.getHeader(headerKey), "Should have set LENGTH header");
                int len = Integer.parseInt(message.getHeader(headerKey));
                assertTrue(len > 0, "Should have a length greater than 0");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSourceHeaderKeyThatWasNotSet() throws Exception {
            String topic = "testSourceHeaderKeyThatWasNotSet";
            String bookmark = "";
            String command = "subscribe";
            String options = "";
            String correlationId = "";
            AMPSSourceHeaderKeys headerKey = AMPSSourceHeaderKeys.TOPIC;
            int messagesToPublish = 1;
            int messagesToWaitFor = messagesToPublish;
            
            AMPSReaderOutput<AMPSMessage> output = getReaderOutput(topic, bookmark, command, options, headerKey, messagesToPublish, messagesToWaitFor, correlationId);
            
            for (AMPSMessage message : output.getRecords()) {
                assertNull(message.getHeader(AMPSSourceHeaderKeys.CORRELATION_ID), "Should have null CORRELATION_ID header");
            }
        }

        private AMPSReaderOutput<AMPSMessage> getReaderOutput(String topic, String bookmark, String command, String options, AMPSSourceHeaderKeys headerKey, int messagesToPublish, int messagesToWaitFor, String correlationId) throws Exception {
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));
            
            String data = getData();

            AMPSReaderOutput<AMPSMessage> output = new AMPSReaderOutput<>();

            AMPSSource<AMPSMessage> source = AMPSSource.<AMPSMessage>builder()
                .setUri(TestConstants.URI) 
                .setTopic(topic)
                .setClientName(topic)
                .setBookmark(bookmark)
                .setDeserializationSchema(new MessageDeserializationSchema())
                .setContentFilter("/data = '" + data + "'")
                .setSubscribeCommand(command)
                .setHeaderKeys(Set.of(headerKey))
                .setOptions(options)
                .build();
            
            try (Client pub = new Client(topic + "-pub");
                    SourceReader<AMPSMessage, AMPSSplit> reader = source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(splits);
                Thread.sleep(READER_SHORT_WAIT_MS);

                pub.connect(TestConstants.URI);
                pub.logon();
                
                for (int i = 0; i < messagesToPublish; i++) {
                    Command cmd = new Command("publish")
                        .setData("{\"id\":" + i + ",\"data\":\"" + data + "\"}")
                        .setTopic(topic)
                        .setCorrelationId(correlationId);
                    pub.executeAsync(cmd, null);
                }

                waitForSpecifiedInput(reader, output, messagesToWaitFor);
            }
            
            assertEquals(messagesToWaitFor, output.getRecords().size(), "Should have received all intended messages");

            return output;
        }
    }

    @Nested
    public class CheckpointSubsumption {
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testBookmarkSubscriptionSubsumesCheckpoints() throws Exception {
            String topic = "testBookmarkSubscriptionSubsumesCheckpoints";
            
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();
            
            String data = getData();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setBookmark("0")
                .setContentFilter("/data = '" + data + "'")
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
            
            try (Client pub = new Client(topic + "-pub");
                    AMPSSourceReader<String> reader = (AMPSSourceReader<String>) source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(splits);
                
                AMPSRecordEmitter<String> emitter = reader.getRecordEmitter();

                pub.connect(TestConstants.URI);
                pub.logon();

                pub.publish(topic, "{\"id\":1,\"data\":\"" + data + "\"}");
                pub.publish(topic, "{\"id\":2,\"data\":\"" + data + "\"}");
                pub.publish(topic, "{\"id\":3,\"data\":\"" + data + "\"}");
                pub.publishFlush(10000L);

                // Simulate checkpoint ID 1, which is not snapshotted/discarded explicitly by the reader
                waitForSpecifiedInput(reader, output, 1);
                emitter.snapshotPendingMessages(1);

                // Simulate checkpoint ID 2, which should have subsumed checkpoint ID 1
                waitForSpecifiedInput(reader, output, 1);
                List<AMPSSplit> checkpoint2 = reader.snapshotState(2);
                reader.notifyCheckpointComplete(2);

                // Simulate checkpoint ID 3, which should have a different bookmark from checkpoint ID 2
                waitForSpecifiedInput(reader, output, 1);
                List<AMPSSplit> checkpoint3 = reader.snapshotState(3);
                reader.notifyCheckpointComplete(3);

                assertEquals(checkpoint2.size(), checkpoint3.size(), "Should have same amount of splits");
                assertNotEquals(checkpoint2.get(0).getBookmark(), checkpoint3.get(0).getBookmark(),
                    "Should not have same bookmark as first checkpoint should have been subsumed");
                assertTrue(emitter.getPendingMessagesByCheckpoint().isEmpty(),
                    "Should have removed all entries to avoid stale messages being kept in memory");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testAtLeastOnceQueueSubsumesCheckpoints() throws Exception {
            String pubTopic = "testAtLeastOnceQueueSubsumesCheckpointsWork";
            String subTopic = "testAtLeastOnceQueueSubsumesCheckpointsWorkToDo";

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(subTopic, ""));

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSSource<String> source = AMPSSource.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(subTopic)
                .setClientName(subTopic)
                .setDeserializationSchema(TestConstants.getStringSchema())
                .setQueueSemantics("at-least-once")
                .setMaxBacklog(2)
                .build();

            try (Client pub = new Client(pubTopic + "-pub");
                    AMPSSourceReader<String> reader = (AMPSSourceReader<String>) source.createReader(new AMPSSourceReaderContext());) {
                reader.start();
                reader.addSplits(splits);
                
                AMPSRecordEmitter<String> emitter = reader.getRecordEmitter();
                
                pub.connect(TestConstants.URI);
                pub.logon();

                pub.publish(pubTopic, "1");
                pub.publish(pubTopic, "2");
                pub.publish(pubTopic, "3");
                pub.publish(pubTopic, "4");
                pub.publishFlush(10000L);
                
                // Simulate checkpoint ID 1, which is not snapshotted/discarded explicitly by the reader
                waitForSpecifiedInput(reader, output, 1);
                emitter.snapshotPendingMessages(1);

                // Simulate checkpoint ID 2, which should subsume checkpoint ID 1
                // and ack both messages
                waitForSpecifiedInput(reader, output, 1);
                List<AMPSSplit> checkpoint2 = reader.snapshotState(2);
                reader.notifyCheckpointComplete(2);

                // Simulate checkpoint ID 3, which should receive two messages
                waitForSpecifiedInput(reader, output, 2);
                List<AMPSSplit> checkpoint3 = reader.snapshotState(3);
                reader.notifyCheckpointComplete(3);

                assertEquals(4, output.getRecords().size(),
                    "Should have received all messages from the queue due to subsumed checkpoints being acked");
            }
        }

    }

    /**
     * Waits for input from a SourceReader and ReaderOutput with a constant timeout and specific amount.
     *
     * @param amount The expected amount of records to wait for.
     */
    private <T> InputStatus waitForSpecifiedInput(SourceReader<T, AMPSSplit> reader, AMPSReaderOutput<T> output, int amount) throws Exception {
        return waitForSpecifiedInput(reader, output, MAX_WAIT_MS, amount);
    }

    /**
     * Waits for input from a SourceReader and ReaderOutput with a timeout or specific amount.
     *
     * @param time The max time to wait for records.
     * @param amount The expected amount of records to wait for.
     */
    private <T> InputStatus waitForSpecifiedInput(SourceReader<T, AMPSSplit> reader, AMPSReaderOutput<T> output, long time, int amount) throws Exception {
        InputStatus status = null;
        long approxTimePassed = 0;
        long sleepTime = 10;
        int startAmount = output.getRecords().size();

        while (approxTimePassed < time && output.getRecords().size() - startAmount < amount && status != InputStatus.END_OF_INPUT) {
            status = reader.pollNext(output);
            
            if (status == InputStatus.NOTHING_AVAILABLE) {
                Thread.sleep(sleepTime);
                approxTimePassed += sleepTime;
            }
        }

        return status;
    }

    private void enableSourceTransport(String clientName) throws Exception {
        Client client = new Client(clientName + "-enabler");

        try {
            client.connect(TestConstants.SOURCE_DISABLE_URI);
            client.logon();
            client.disconnect();
        } catch (ConnectionRefusedException cre) {
            client.connect(TestConstants.URI);
            client.logon();

            client.publish("enableSourceServerChooserTransport", "");
            client.publishFlush(10000L);

            client.disconnect();
        } finally {
            client.close();
        }
    }

    private int getRecordId(AMPSReaderOutput<?> output, int index) {
        return Integer.parseInt(output.getRecords().get(index).toString().split(",")[0].split(":")[1]);
    }

    private String getData() {
        return "d" + System.currentTimeMillis();
    }
}

