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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.FailedWriteHandler;
import com.crankuptheamps.client.FixedDelayStrategy;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.MemoryPublishStore;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.exception.ConnectionException;
import com.crankuptheamps.client.exception.ConnectionRefusedException;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.sink.metrics.SinkMetrics;
import com.crankuptheamps.flink.sink.writer.AMPSStatefulSinkWriter;
import com.crankuptheamps.flink.sink.writer.AMPSWriterState;
import com.crankuptheamps.flink.sink.writer.serializer.AMPSSerializationSchema;
import com.crankuptheamps.flink.testutils.AMPSMessageHandler;
import com.crankuptheamps.flink.testutils.AMPSPojo;
import com.crankuptheamps.flink.testutils.AMPSWriterInitContext;
import com.crankuptheamps.flink.testutils.MessageSerializationSchema;
import com.crankuptheamps.flink.testutils.SSLConnectorInitializer;
import com.crankuptheamps.flink.testutils.TestConstants;
import com.crankuptheamps.flink.util.SerializedElement;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.metrics.Counter;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSStatefulSinkWriterTest {
    
    public static int MH_QUEUE_POLL_TIMEOUT_MS = 1000;

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

    @Nested
    public class DeliveryGuarantees {
    
        @Test
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testAtLeastOnce() throws Exception {
            String topic = "testAtLeastOnce";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();

                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
               
                writer.write("1", new WriterContext());
                writer.write("2", new WriterContext());

                assertNull(mh.queue.poll(100, TimeUnit.MILLISECONDS), "Should not have received a message");

                writer.flush(false);

                assertEquals("1", mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received first message");
                assertEquals("2", mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received second message");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testExactlyOnce() throws Exception {
            String topic = "testExactlyOnce";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setUseSuffix(true)
                .build();
            SinkWriter<String> restoredWriter = null;

            List<String> messages = List.of("1", "2", "3");
            List<WriterContext> contexts = List.of(
                new WriterContext(1, 1),
                new WriterContext(2, 2),
                new WriterContext(3, 3));

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();

                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);

                writer.write(messages.get(0), contexts.get(0));
                writer.write(messages.get(1), contexts.get(1));

                assertNull(mh.queue.poll(100, TimeUnit.MILLISECONDS), "Should not have received a message");

                writer.flush(false);

                assertEquals(messages.get(0), mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received first message");
                assertEquals(messages.get(1), mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received second message");

                List<AMPSWriterState> states = ((AMPSStatefulSinkWriter<String>) writer).snapshotState(1);

                restoredWriter = sink.restoreWriter(new AMPSWriterInitContext(), states);
                
                restoredWriter.write(messages.get(0), contexts.get(0));
                restoredWriter.write(messages.get(1), contexts.get(1)); 
                restoredWriter.write(messages.get(2), contexts.get(2));
                restoredWriter.flush(false);

                assertEquals(messages.get(2), mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received third message");
                assertTrue(mh.queue.isEmpty(), "Should have received exactly three messages");
            } finally {
                if (restoredWriter != null) {
                    restoredWriter.close();
                }
            }
        }
        
        @Test
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testExactlyOnceSnapshotsRestoredState() throws Exception {
            String topic = "testExactlyOnceSnapshotsRestoredState";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setUseSuffix(true)
                .build();
            StatefulSinkWriter<String, AMPSWriterState> restoredWriter = null;

            List<String> messages = List.of("1");
            List<WriterContext> contexts = List.of(new WriterContext(1, 1));

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();

                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);

                writer.write(messages.get(0), contexts.get(0));

                assertNull(mh.queue.poll(100, TimeUnit.MILLISECONDS), "Should not have received a message");

                writer.flush(false);

                assertEquals(messages.get(0), mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received first message");

                List<AMPSWriterState> states = ((AMPSStatefulSinkWriter<String>) writer).snapshotState(1);

                restoredWriter = sink.restoreWriter(new AMPSWriterInitContext(), states);

                List<AMPSWriterState> statesWithoutSnapshot = restoredWriter.snapshotState(1);

                AMPSWriterState stateWithFlush = states.get(0);
                AMPSWriterState stateWithoutFlush = statesWithoutSnapshot.get(0);

                assertEquals(stateWithFlush.getLastTimestamp(),
                    stateWithoutFlush.getLastTimestamp(),
                    "Should have the same timestamp from recovered state");
            } finally {
                if (restoredWriter != null) {
                    restoredWriter.close();
                }
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testExactlyOnceWithoutTimestampsShouldThrowException() throws Exception {
            String topic = "testExactlyOnceWithoutTimestampsShouldThrowException";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                assertThrows(UnsupportedOperationException.class, () -> writer.write("1", new WriterContext()),
                    "Should throw exception when no timestamps are included");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testExactlyOnceWithoutStrictlyIncreasingTimestampsShouldThrowException() throws Exception {
            String topic = "testExactlyOnceWithoutStrictlyIncreasingTimestampsShouldThrowException";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

            List<String> messages = List.of("1", "2");
            List<WriterContext> contexts = List.of(
                new WriterContext(1, 1),
                new WriterContext(2, 2));

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                writer.write(messages.get(0), contexts.get(0));
                writer.write(messages.get(1), contexts.get(1));

                assertThrows(UnsupportedOperationException.class, () -> {
                    writer.write(messages.get(1), contexts.get(1));
                }, "Should throw exception on equal timestamp to last timestamp");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testExactlyOnceAfterRecoveryShouldThrowExceptionOnInvalidTimestamp() throws Exception {
            String topic = "testExactlyOnceAfterRecoveryShouldThrowExceptionOnInvalidTimestamp";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setUseSuffix(true)
                .build();

            List<String> messages = List.of("1", "2", "3");
            List<WriterContext> contexts = List.of(
                new WriterContext(1, 1),
                new WriterContext(2, 2),
                new WriterContext(3, 3));

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                writer.write(messages.get(0), contexts.get(0));
                writer.write(messages.get(1), contexts.get(1));

                List<AMPSWriterState> states = ((AMPSStatefulSinkWriter<String>) writer).snapshotState(1);

                try (SinkWriter<String> restoredWriter = sink.restoreWriter(new AMPSWriterInitContext(), states);) {
                    // Writer has not recovered yet, so these messages should be dropped without throwing an exception
                    restoredWriter.write(messages.get(0), contexts.get(0));
                    restoredWriter.write(messages.get(1), contexts.get(1));

                    restoredWriter.write(messages.get(2), contexts.get(2));

                    assertThrows(UnsupportedOperationException.class, () -> {
                        restoredWriter.write(messages.get(0), contexts.get(0));
                    }, "Should throw exception on earlier timestamp to last timestamp after recovery");
                    assertThrows(UnsupportedOperationException.class, () -> {
                        restoredWriter.write(messages.get(2), contexts.get(2));
                    }, "Should throw exception on equal timestamp to last timestamp after recovery");
                }
            }
        }
    }

    @Nested
    public class Publish {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testWriteAdhoc() throws Exception {
            String topic = "testWriteAdhoc";
        
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();
        
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
        
                writer.write("1", new WriterContext());
        
                assertEquals("1", mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received message");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testWriteJsonAdhoc() throws Exception {
            String topic = "testWriteJsonAdhoc";
        
            AMPSSink<AMPSPojo> sink = AMPSSink.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                .build();
        
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<AMPSPojo> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
        
                AMPSPojo initialPojo = new AMPSPojo(123, "test123");
                writer.write(initialPojo, new WriterContext());
        
                String message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(message, "Should have received Pojo message");
                DeserializationSchema<AMPSPojo> ds = new JsonDeserializationSchema<>(AMPSPojo.class);
                ds.open(null);

                assertEquals(initialPojo, ds.deserialize(message.getBytes()), "Should have equal Pojos");
            }
        }
        
        @Test
        @ResourceLock(value = "DISABLE_SINK_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testWriteWithPublishStoreFailover() throws Exception {
            String topic = "testWriteWithPublishStoreFailover";
        
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setServerChooserSupplier(() -> {
                    DefaultServerChooser sc = new DefaultServerChooser();
                    sc.add(TestConstants.SINK_DISABLE_URI);
                    sc.add(TestConstants.URI);
                    return sc;
                })
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setPublishStoreFunction((name) -> {
                    try {
                        return new MemoryPublishStore(10);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();
    
            enableSinkTransport(topic);

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("1", new WriterContext());
                writer.write("2", new WriterContext());
                writer.write("3", new WriterContext());

                sub.publish("disableSinkServerChooserTransport", "");
                sub.publishFlush(10000L);

                writer.write("4", new WriterContext());
                writer.write("5", new WriterContext());
                writer.write("6", new WriterContext());
                writer.flush(false);

                List<String> messages = new ArrayList<>();
                Set<String> uniqueMessages = new HashSet<>();
                while (uniqueMessages.size() < 6) {
                    String message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    assertNotNull(message, "Should not have polled null.");
                    uniqueMessages.add(message);
                    messages.add(message);
                }
                assertEquals(6, uniqueMessages.size(), "Should have received all messages.");
                assertEquals(6, messages.size(), "Should have no duplicates.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testWithoutSetCorrelationId() throws Exception {
            String topic = "testWithoutSetCorrelationId";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();
            
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("1", new WriterContext());
                writer.write("2", new WriterContext());
                writer.flush(false);

                List<String> messages = new ArrayList<>();
                while (messages.size() < 2) {
                    String message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    assertNotNull(message, "Should not have polled null.");
                    messages.add(message);
                }
                assertEquals(2, messages.size(), "Should have received all intended messages.");
                assertEquals(0, mh.correlationIds.size(), "Should have no correlation IDs.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSetCorrelationIdInBuilder() throws Exception {
            String topic = "testSetCorrelationIdInBuilder";

            String correlationId = "correlationId";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setCorrelationId(correlationId)
                .build();
            
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("1", new WriterContext());
                writer.flush(false);

                String cId = mh.correlationIds.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                
                assertNotNull(cId, "Should have received correlation ID");
                assertEquals(correlationId, cId, "Should have same correlation ID");
            }
        }
    }
    
    @Nested
    public class Metrics {

        @Test
        @ResourceLock(value = "DISABLE_SINK_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSinkConnectsAndDisconnects() throws Exception {
            String topic = "testSinkConnectsAndDisconnects";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.SINK_DISABLE_URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();
            AMPSWriterInitContext context = new AMPSWriterInitContext();

            enableSinkTransport(topic);
            
            try (Client pub = new Client(topic + "-pub");
                    SinkWriter<String> writer = sink.createWriter(context);) {
                pub.connect(TestConstants.URI);
                pub.logon();
                
                Metric connects = context.metricGroup.getMetric(SinkMetrics.Metric.CONNECTS);
                assertNotNull(connects, "Should have found connects metric.");
                assertEquals(MetricType.COUNTER, connects.getMetricType(), "Should be a Counter metric.");

                Metric disconnects = context.metricGroup.getMetric(SinkMetrics.Metric.DISCONNECTS);
                assertNotNull(disconnects, "Should have found disconnects metric.");
                assertEquals(MetricType.COUNTER, disconnects.getMetricType(), "Should be a Counter metric.");

                while (((Counter) connects).getCount() == 0) {
                    Thread.sleep(20);
                }
                
                assertEquals(1, ((Counter) connects).getCount(), "Should have connected.");
                assertEquals(0, ((Counter) disconnects).getCount(), "Should not have disconnected.");
                
                pub.publish("disableSinkServerChooserTransport", "");
                pub.publishFlush(10000L);
                
                while (((Counter) disconnects).getCount() == 0) {
                    Thread.sleep(20);
                }
                assertEquals(1, ((Counter) connects).getCount(), "Should only have one connect.");
                assertTrue(1 <= ((Counter) disconnects).getCount(), "Should have disconnected.");

                pub.publish("enableSinkServerChooserTransport", "");
                pub.publishFlush(10000L);

                while (((Counter) connects).getCount() == 1) {
                    Thread.sleep(20);
                }
                assertEquals(2, ((Counter) connects).getCount(), "Should have two connects.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testRecordsSentToAMPS() throws Exception {
            String topic = "testRecordsSentToAMPS";
            
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();
            AMPSWriterInitContext context = new AMPSWriterInitContext();

            try (SinkWriter<String> writer = sink.createWriter(context);) {
                Counter records = context.metricGroup.getNumRecordsSendCounter();
                assertEquals(0, records.getCount(), "Should have correct records count.");

                Counter bytes = context.metricGroup.getNumBytesSendCounter();
                assertEquals(0, bytes.getCount(), "Should have correct bytes count.");
                
                String data = "123";

                writer.write(data, new WriterContext());

                assertEquals(1, records.getCount(), "Should have incremented records counter.");
                assertEquals(data.getBytes().length, bytes.getCount(), "Should have increased byte counter.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testRecordsSentToAMPSErrors() throws Exception {
            String topic = "testRecordsSentToAMPSErrors";
            
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();
            AMPSWriterInitContext context = new AMPSWriterInitContext();

            try (SinkWriter<String> writer = sink.createWriter(context);) {
                Counter outErrs = context.metricGroup.getNumRecordsOutErrorsCounter();
                assertEquals(0, outErrs.getCount(), "Should have correct out errors count.");

                Counter sendErrs = context.metricGroup.getNumRecordsSendErrorsCounter();
                assertEquals(0, sendErrs.getCount(), "Should have correct send errors count.");
         
                try {
                    writer.close();
                    writer.write("1", new WriterContext());
                } catch (Exception e) {}

                assertEquals(1, outErrs.getCount(), "Should have incremented out errors counter.");
                assertEquals(1, sendErrs.getCount(), "Should have incremented send errors counter.");
            }
        }
    }
    
    @Nested
    public class SOW {

        @Test
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWExpiration() throws Exception {
            String topic = "testSOWExpiration";

            final int expiration = 1;
            
            AMPSSink<AMPSPojo> sink = AMPSSink.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-pub")
                .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                .setExpiration(expiration)
                .build();

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<AMPSPojo> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSPojo pojo = new AMPSPojo(1, "1", 1);
                
                writer.write(pojo, new WriterContext());

                int count = 0;
                int attempts = 0;
    
                while (attempts < 20 && count != 3) {
                    AMPSMessageHandler mh = new AMPSMessageHandler();
                    sub.sow(mh, topic, 0);
                    count = 0;
                    while (true) {
                        String msg = mh.queue.poll(100, TimeUnit.MILLISECONDS);
                        if (msg == null) break;
                        count++;
                    }
                    attempts++;
                }
                assertEquals(3, count, "Should have received group begin/end and SOW record.");

                Thread.sleep(expiration);
                
                attempts = 0;
                count = 0;

                // Make sure the SOW entry expires by trying to subscribe multiple times
                while (attempts < 20 && count != 2) {
                    AMPSMessageHandler mh = new AMPSMessageHandler();
                    sub.sow(mh, topic, 0);
                    count = 0;
                    while (true) {
                        String msg = mh.queue.poll(100, TimeUnit.MILLISECONDS);
                        if (msg == null) break;
                        count++;
                    }
                    attempts++;
                }
                assertEquals(2, count, "Should have only received group begin/end.");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testDeltaPublish() throws Exception {
            String topic = "testDeltaPublish";

            AMPSSink<AMPSPojo> sinkInit = AMPSSink.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-pub")
                .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                .build();
            AMPSSink<String> sinkDelta = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-pubD")
                .setSerializationSchema(TestConstants.getStringSchema())
                .setPublishCommand("delta_publish")
                .build();

            try (Client sub = new Client(topic + "-sub"); Client subD = new Client(topic + "-subD");
                    SinkWriter<AMPSPojo> writerInitial = sinkInit.createWriter(new AMPSWriterInitContext());
                    SinkWriter<String> writerDelta = sinkDelta.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();

                TestConstants.sowDelete(sub, topic);

                subD.connect(TestConstants.URI);
                subD.logon();
                
                AMPSPojo initialPojo = new AMPSPojo(1, "1", 1);
                writerInitial.write(initialPojo, new WriterContext());
                writerInitial.flush(false);
                
                AMPSMessageHandler dmh = new AMPSMessageHandler();
                AMPSMessageHandler mh = new AMPSMessageHandler();
                subD.deltaSubscribe(dmh, topic, "/id = 1", 0);
                sub.subscribe(mh, topic, "/id = 1", 0);

                String deltaPojoStr = "{\"id\":" + initialPojo.id + ",\"num\":" + (initialPojo.num + 1) + "}";
                writerDelta.write(deltaPojoStr, new WriterContext());
                
                String message = dmh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertEquals(deltaPojoStr, message, "Delta sub should have received correct delta message.");

                message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                AMPSPojo updatedPojo = new AMPSPojo(initialPojo.id, initialPojo.data, initialPojo.num + 1);
                DeserializationSchema<AMPSPojo> ds = new JsonDeserializationSchema<>(AMPSPojo.class);
                ds.open(null);
                assertEquals(updatedPojo, ds.deserialize(message.getBytes()), "Sub should have received the correct pojo.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSOWDeleteByData() throws Exception {
            String topic = "testSOWDeleteByData";

            AMPSSink<AMPSPojo> sinkInit = AMPSSink.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-pub")
                .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                .build();
            AMPSSink<AMPSPojo> sinkDelete = AMPSSink.<AMPSPojo>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-pubDelete")
                .setSerializationSchema(TestConstants.getAMPSPojoSerializer())
                .setPublishCommand("sow_delete")
                .build();

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<AMPSPojo> writerInitial = sinkInit.createWriter(new AMPSWriterInitContext());
                    SinkWriter<AMPSPojo> writerDelete = sinkDelete.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();

                TestConstants.sowDelete(sub, topic);

                AMPSPojo initialPojo = new AMPSPojo(1, "1", 1);
                writerInitial.write(initialPojo, new WriterContext());
                writerInitial.flush(false);

                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.sow(mh, topic, "/id = 1", 1, 0);
                // Group begin
                assertNotNull(mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have received group begin");
                // Message
                String message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(message, "Should have received SOW record");
                assertNotEquals("", message, "Should not be an empty string");
                mh.queue.clear();
                    
                DeserializationSchema<AMPSPojo> ds = new JsonDeserializationSchema<>(AMPSPojo.class);
                ds.open(null);
                assertEquals(initialPojo, ds.deserialize(message.getBytes()), "Sub should have received the correct pojo.");
                
                writerDelete.write(initialPojo, new WriterContext());
                writerDelete.flush(false);

                int attempts = 0;
                int count = 0;

                while (attempts < 20 && count != 2) {
                    mh = new AMPSMessageHandler();
                    sub.sow(mh, topic, "/id = 1", 1, 0);
                    count = 0;
                    while (true) {
                        String msg = mh.queue.poll(100, TimeUnit.MILLISECONDS);
                        if (msg == null) break;
                        count++;
                    }
                    attempts++;
                }

                assertEquals(2, count, "Should have only received group begin and end");
            }
        }
    }

    @Nested
    public class ClientSetUp {
       
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testRetryOnDisconnect() throws Exception {
            String topic = "testRetryOnDisconnect";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-set")
                .setSerializationSchema(TestConstants.getStringSchema())
                .setRetryOnDisconnect(false)
                .build();
            
            AMPSSink<String> sinkDefault = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-default")
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());
                    SinkWriter<String> writerDefault = sinkDefault.createWriter(new AMPSWriterInitContext());) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();
                HAClient writerDefaultClient = ((AMPSStatefulSinkWriter<String>) writerDefault).getClient();

                assertFalse(writerClient.getRetryOnDisconnect(), "Should have false retry on disconnect if set.");
                
                assertTrue(writerDefaultClient.getRetryOnDisconnect(), "Should have true retry on disconnect by default.");
            }
        }

        @Test
        @ResourceLock(value = "DISABLE_SINK_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSinkReconnectDelayStrategy() throws Exception {
            String topic = "testSinkReconnectDelayStrategy";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.SINK_DISABLE_URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setReconnectDelayStrategySupplier(() -> new FixedDelayStrategy(1, 5))
                .build();

            disableSinkTransport(topic);

            assertThrows(RuntimeException.class, () -> { 
                try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                    writer.flush(true);
                }
            }, "Should throw a RuntimeException since transport should be disabled.");
        }
        
        @Test
        @ResourceLock(value = "DISABLE_SINK_TRANSPORT", mode = ResourceAccessMode.READ_WRITE)
        @Timeout(value = TestConstants.NORMAL_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSinkServerChooserFlush() throws Exception {
            String topic = "testSinkServerChooserFlush";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setTopic(topic)
                .setClientName(topic + "-pub")
                .setSerializationSchema(TestConstants.getStringSchema())
                .setServerChooserSupplier(() -> {
                    DefaultServerChooser sc = new DefaultServerChooser();
                    sc.add(TestConstants.SINK_DISABLE_URI);
                    sc.add(TestConstants.URI);
                    return sc;
                })
                .build();
            
            enableSinkTransport(topic);

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("a", new WriterContext());
                writer.flush(false);

                sub.publish("disableSinkServerChooserTransport", "");
                sub.publishFlush(10000L);

                writer.write("b", new WriterContext());
                writer.flush(false);

                assertEquals(2, mh.queue.size(), "Should have received both messages.");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testFailedWriteHandler() throws Exception {
            String topic = "testFailedWriteHandler";

            class FWH implements FailedWriteHandler {
                public static final LinkedBlockingQueue<String> msgs = new LinkedBlockingQueue<>(5);

                @Override
                public void failedWrite(Message message, int reason) {
                    msgs.offer(message.getData());
                }
            }

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-pub")
                .setSerializationSchema(TestConstants.getStringSchema())
                .setFailedWriteHandlerSupplier(() -> new FWH())
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();

                assertInstanceOf(FWH.class, writerClient.getFailedWriteHandler(), "Should have set FailedWriteHandler.");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testHeartbeatWithInterval() throws Exception {
            String topic = "testHeartbeatWithInterval";
    
            final int intervalSeconds = 5;

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setHeartbeat(intervalSeconds)
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();

                assertEquals(intervalSeconds, writerClient.getHeartbeatInterval(), "Should be same interval.");
                assertEquals(intervalSeconds * 2, writerClient.getReadTimeout(), "Should be double the interval.");
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

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setExceptionListenerSupplier(() -> new EL())
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();

                assertInstanceOf(EL.class, writerClient.getExceptionListener(), "The exception handler should have been set.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testUseSuffixTrue() throws Exception {
            String topic = "testUseSuffixTrue";
    
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setUseSuffix(true)
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();

                assertTrue(writerClient.getName().contains(topic), "Should contain the client name");
                assertNotEquals(writerClient.getName(), topic, "Should not be equal due to the suffix");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testUseSuffixFalse() throws Exception {
            String topic = "testUseSuffixFalse";
    
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setUseSuffix(false)
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();

                assertEquals(topic, writerClient.getName(), "Should have the same client name");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testUseSuffixParallelism() throws Exception {
            String topic = "testUseSuffixParallelism";
    
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .build();

            try (SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext(2));) {
                HAClient writerClient = ((AMPSStatefulSinkWriter<String>) writer).getClient();

                assertTrue(writerClient.getName().contains(topic), "Should contain the client name");
                assertNotEquals(writerClient.getName(), topic, "Should not be equal due to the suffix");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSinkSSLConnectorInitializer() throws Exception {
            String topic = "testSinkSSLConnectorInitializer";
            
            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.S_URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(TestConstants.getStringSchema())
                .setConnectorInitializer(new SSLConnectorInitializer())
                .setReconnectDelayStrategySupplier(() -> new FixedDelayStrategy(50, 4)) // Client will hang without this
                .build();
            
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("1", new WriterContext());
                writer.flush(false);

                String message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(message, "Should not have polled null");
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
    public class SerializationSchema {
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSetCorrelationIdInSerializationSchema() throws Exception {
            String topic = "testSetCorrelationIdInSerializationSchema";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(new MessageSerializationSchema<>())
                .build();
            
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("1", new WriterContext());
                writer.write("2", new WriterContext());
                writer.flush(false);

                List<String> messages = new ArrayList<>();
                while (messages.size() < 2) {
                    String message = mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    assertNotNull(message, "Should not have polled null.");
                    messages.add(message);
                }
                assertEquals(2, messages.size(), "Should have received all intended messages.");
                assertEquals(messages.size(), mh.correlationIds.size(), "Should have received all correlation IDs.");
            }
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSetCorrelationIdInSerializationSchemaOverridesBuilder() throws Exception {
            String topic = "testSetCorrelationIdInSerializationSchemaOverridesBuilder";

            String correlationId = "correlationId";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(new MessageSerializationSchema<>())
                .setCorrelationId(correlationId)
                .build();
            
            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                
                AMPSMessageHandler mh = new AMPSMessageHandler();
                sub.subscribe(mh, topic, 0);
    
                writer.write("1", new WriterContext());
                writer.flush(false);

                String cId = mh.correlationIds.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                
                assertNotNull(cId, "Should have received correlation ID");
                assertNotEquals(correlationId, cId, "Should have overridden builder defined correlation ID");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSetSowKeyInSerializationSchemaForPublish() throws Exception {
            String topic = "testSetSowKeyInSerializationSchemaForPublish";

            String sowKey = "0";

            AMPSSink<String> sink = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic)
                .setSerializationSchema(new AMPSSerializationSchema<String>() {
                    @Override
                    public SerializedElement serialize(String element, SinkWriter.Context context) {
                        SerializedElement se = new SerializedElement(element.getBytes());
                        se.setSowKey(sowKey);
                        return se;
                    }
                })
                .build();

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writer = sink.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                TestConstants.sowDelete(sub, topic);

                AMPSMessageHandler mh = new AMPSMessageHandler();

                writer.write("{\"id\":1,\"data\":\"d\"}", new WriterContext());
                writer.flush(false);

                sub.sow(mh, topic, 0);

                String sk = mh.sowKeys.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                assertNotNull(sk, "Should have received SOW Key");
                assertEquals(sowKey, sk, "Should have the set SOW Key");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testSetSowKeyInSerializationSchemaForDelete() throws Exception {
            String topic = "testSetSowKeyInSerializationSchemaForDelete";

            String sowKey = "0";

            AMPSSink<String> sinkInit = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-init")
                .setSerializationSchema(new AMPSSerializationSchema<String>() {
                    @Override
                    public SerializedElement serialize(String element, SinkWriter.Context context) {
                        SerializedElement se = new SerializedElement(element.getBytes());
                        se.setSowKey(sowKey);
                        return se;
                    }
                })
                .build();
            AMPSSink<String> sinkDelete = AMPSSink.<String>builder()
                .setUri(TestConstants.URI)
                .setTopic(topic)
                .setClientName(topic + "-delete")
                .setSerializationSchema(new AMPSSerializationSchema<String>() {
                    @Override
                    public SerializedElement serialize(String element, SinkWriter.Context context) {
                        SerializedElement se = new SerializedElement();
                        se.setSowKey(sowKey);
                        return se;
                    }
                })
                .setPublishCommand("sow_delete")
                .build();

            try (Client sub = new Client(topic + "-sub");
                    SinkWriter<String> writerInit = sinkInit.createWriter(new AMPSWriterInitContext());
                    SinkWriter<String> writerDelete = sinkDelete.createWriter(new AMPSWriterInitContext());) {
                sub.connect(TestConstants.URI);
                sub.logon();
                TestConstants.sowDelete(sub, topic);

                AMPSMessageHandler mh = new AMPSMessageHandler();

                writerInit.write("{\"id\":1,\"data\":\"d\"}", new WriterContext());
                writerInit.flush(false);

                sub.sow(mh, topic, 0);

                assertNotNull(mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have group begin");
                assertNotNull(mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have SOW message");
                assertNotNull(mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have group end");

                String sk = mh.sowKeys.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                assertNotNull(sk, "Should have received SOW Key");
                assertEquals(sowKey, sk, "Should have the set SOW Key");

                mh.queue.clear();

                writerDelete.write("", new WriterContext());
                writerDelete.flush(false);
                
                sub.sow(mh, topic, 0);

                assertNotNull(mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have group begin");
                assertNotNull(mh.queue.poll(MH_QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Should have group end");
                assertNull(mh.queue.poll(100, TimeUnit.MILLISECONDS), "Should have no more messages");
            }
        }
    }

    private void enableSinkTransport(String clientName) throws Exception {
        Client client = new Client(clientName + "-enabler");

        try {
            client.connect(TestConstants.SINK_DISABLE_URI);
            client.logon();
            client.disconnect();
        } catch (ConnectionRefusedException cre) {
            client.connect(TestConstants.URI);
            client.logon();
        
            client.publish("enableSinkServerChooserTransport", "");
            client.publishFlush(10000L);
        
            client.disconnect();
        } finally {
            client.close();
        }
    }

    private void disableSinkTransport(String clientName) throws Exception {
        Client client = new Client(clientName + "-disabler");
        try {
            client.connect(TestConstants.SINK_DISABLE_URI);
            client.logon();
            
            client.publish("disableSinkServerChooserTransport", "");
            client.publishFlush(10000L);

            client.disconnect();
        } catch (ConnectionRefusedException cre) {
            client.disconnect();
        } finally {
            client.close();
        }
    }

    private static class WriterContext implements SinkWriter.Context {
        private final long watermark;
        private final Long timestamp;

        public WriterContext() {
            watermark = 1;
            timestamp = null;
        }

        public WriterContext(long watermark, long timestamp) {
            this.watermark = watermark;
            this.timestamp = timestamp;
        }

        @Override
        public long currentWatermark() {
            return watermark;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }
}

