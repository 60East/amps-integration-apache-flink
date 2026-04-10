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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.flink.source.reader.AMPSSplitReader;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.testutils.AMPSSourceReaderContext;
import com.crankuptheamps.flink.testutils.TestConstants;
import com.crankuptheamps.flink.util.AMPSMessage;
import com.crankuptheamps.flink.util.AMPSQueueSemantics;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSSplitReaderTest {
    
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
    public class Fetch {

        @Test
        @Timeout(value = TestConstants.LONG_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testShouldNotHangWithMessagesInBuffer() throws Exception {
            String topic = "testShouldNotHangWithMessagesInBuffer";

            final int messages = 50000;

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            try (Client pub = new Client(topic + "-pub");
                    DummyAMPSSplitReader reader = makeReaderWithBufferSize(topic, 1);) {
                reader.handleSplitsChanges(new SplitsAddition<>(splits));
                Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for client

                pub.connect(TestConstants.URI);
                pub.logon();

                CompletableFuture<Boolean> fetchFuture = CompletableFuture.supplyAsync(() -> {
                    try {
                        long lastTime = System.currentTimeMillis();
                        long currentTime = 0;
                        int recordsRead = 0;
                        for (int i = 0; i < messages; i++) {
                            pub.publish(topic, "1");
                            pub.publish(topic, "2");
                            RecordsWithSplitIds records = reader.fetch();
                            currentTime = System.currentTimeMillis();
                            records.nextSplit();
                            Object msg = records.nextRecordFromSplit();
                            if (currentTime - lastTime >= 500) {
                                if (msg != null) {
                                    // Reader properly stopped blocking and test can exit
                                    return true;
                                }
                            }
                            if (msg != null) {
                                recordsRead++;
                            }
                            lastTime = currentTime;
                        }
                        // Reader did not hang at all
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                CompletableFuture<Boolean> sleepFuture = CompletableFuture.supplyAsync(() -> {
                    try {
                        TimeUnit.SECONDS.sleep(TestConstants.LONG_TIMEOUT - 2);
                    } catch (InterruptedException ie) {}
                    return false;
                });

                CompletableFuture<Object> firstResponse = CompletableFuture.anyOf(fetchFuture, sleepFuture);
                
                assertTrue((boolean) firstResponse.get(), 
                    "Should not hang during fetch call when messages are in the buffer");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testRemovesPoisonedMessageWithOneSplit() throws Exception {
            String topic = "testRemovesPoisonedMessageWithOneSplit";
            
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));

            poisonMessageTest(topic, splits);
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testRemovesPoisonedMessageWithMultipleSplits() throws Exception {
            String topic = "testRemovesPoisonedMessageWithMultipleSplits";
            
            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(topic));
            splits.add(new AMPSSplit(topic));

            poisonMessageTest(topic, splits);
        }

        private void poisonMessageTest(String topic, List<AMPSSplit> splits) throws Exception {
            try (DummyAMPSSplitReader reader = makeReaderWithBufferSize(topic, 10);) {
                reader.handleSplitsChanges(new SplitsAddition<>(splits));
                Thread.sleep(READER_SHORT_WAIT_MS); // Brief wait for client

                // Check if the poisoned message will be emitted if it is not the head
                AMPSMessage msg = new AMPSMessage();
                msg.setSplitIndex(0);

                reader.addMessage(msg);
                reader.addPoison();

                RecordsWithSplitIds records = reader.fetch();
                records.nextSplit();

                AMPSMessage msgFromReader = (AMPSMessage) records.nextRecordFromSplit();
                assertNotNull(msgFromReader, "Should have a message");
                assertEquals(msg, msgFromReader, "Should have the added message");

                AMPSMessage poisonMessage = (AMPSMessage) records.nextRecordFromSplit();
                assertNull(poisonMessage, "Should not have the poison message");

                // Check if the poisoned message will be emitted if it is the head
                reader.addPoison();
                reader.addMessage(msg);

                records = reader.fetch();
                records.nextSplit();

                msgFromReader = (AMPSMessage) records.nextRecordFromSplit();
                assertNotEquals(reader.getPoison(), msgFromReader, "Should not have the poison message");
                assertEquals(msg, msgFromReader, "Should have the added message");
            }
        }
    }
    
    private DummyAMPSSplitReader makeReaderWithBufferSize(String topic, int bufferSize) {
        return new DummyAMPSSplitReader(topic, bufferSize);
    }

    private static class DummyAMPSSplitReader extends AMPSSplitReader {
        
        public DummyAMPSSplitReader(String topic, int bufferSize) {
            super(
                new AMPSSourceReaderContext(),
                TestConstants.URI,
                topic,
                topic + "-sub",
                null,   // filter
                null,   // options
                null,   // bookmark store function
                null,   // server chooser
                null,   // bookmark
                AMPSQueueSemantics.NONE,
                10,     // ack batch size
                0,      // ack timeout
                0,      // topn
                DeliveryGuarantee.NONE,
                "subscribe",
                null,       // exception listener
                bufferSize, // internal buffer size
                10,         // batch size
                null,       // order by
                null,       // reconnect delay strategy
                0,          // heartbeat
                new HAClient(),
                0,      // skipn
                0,      // maxbacklog
                false,  // use suffix
                0,      // sleep millis after block
                new HashSet<>(),
                null    // init callback
            );
        }

        public void addMessage(AMPSMessage message) {
            messages.offer(message);
        }

        public void addPoison() {
            poisoned = true;
            messages.offer(poison);
        }

        public AMPSMessage getPoison() {
            return poison;
        }
    }

    private String getData() {
        return "d" + System.currentTimeMillis();
    }
}

