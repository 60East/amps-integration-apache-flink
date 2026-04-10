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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.client.DefaultBookmarkStore;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.fields.Field;
import com.crankuptheamps.flink.source.reader.AMPSRecordEmitter;
import com.crankuptheamps.flink.source.reader.deserializer.AMPSDataOnlyDeserializationSchemaWrapper;
import com.crankuptheamps.flink.source.split.AMPSSplitState;
import com.crankuptheamps.flink.testutils.AMPSPojo;
import com.crankuptheamps.flink.testutils.AMPSReaderOutput;
import com.crankuptheamps.flink.testutils.AMPSSourceReaderContext;
import com.crankuptheamps.flink.testutils.TestConstants;
import com.crankuptheamps.flink.util.AMPSMessage;
import com.crankuptheamps.flink.util.AMPSQueueSemantics;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AMPSRecordEmitterTest {

    @Nested
    public class Timestamp {
        private static final byte[] ts1 = "20210101T010101.999999Z".getBytes(StandardCharsets.UTF_8);
        private static final long ts1Millis = 1609462861999L;
        private static final byte[] ts2 = "20210101T010102.000000Z".getBytes(StandardCharsets.UTF_8); 
        private static final long ts2Millis = 1609462862000L;
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testConvertTimestamp() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter("", AMPSQueueSemantics.NONE);

            long firstTimestamp = emitter.convertTimestamp(ts1);
            long secondTimestamp = emitter.convertTimestamp(ts2);

            assertEquals(ts1Millis, firstTimestamp, "Should be same timestamp");
            assertEquals(ts2Millis, secondTimestamp, "Should be same timestamp");
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testToEpochMillisUTCNotLeapYear() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter("", AMPSQueueSemantics.NONE);

            long timestampBeforeFeb = emitter.toEpochMillisUTC(2022, 1, 1, 1, 1, 1, 0);
            long timestampAfterFeb = emitter.toEpochMillisUTC(2022, 12, 1, 1, 1, 1, 0);

            assertEquals(1640998861000L, timestampBeforeFeb, "Should be same timestamp before February");
            assertEquals(1669856461000L, timestampAfterFeb, "Should be same timestamp after February");
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testToEpochMillisUTCLeapYear() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter("", AMPSQueueSemantics.NONE);

            long timestampBeforeFeb = emitter.toEpochMillisUTC(2024, 2, 8, 1, 1, 1, 0);
            long timestampAfterFeb = emitter.toEpochMillisUTC(2024, 11, 30, 1, 1, 1, 0);

            assertEquals(1707354061000L, timestampBeforeFeb, "Should be same timestamp before February");
            assertEquals(1732928461000L, timestampAfterFeb, "Should be same timestamp after February");
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testEmitWithTimestamp() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter(Message.Options.Timestamp, AMPSQueueSemantics.NONE);

            AMPSMessage message = buildMessage("data");
            message.setTimestamp(ts1);

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            doEmitRecord(emitter, message, output);
   
            assertEquals(1, output.getRecords().size(), "Should have emitted one record");
            assertEquals(1, output.getTimestamps().size(), "Should have emitted with one timestamp");
        }
    }

    @Nested
    public class Emit {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testEmitString() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter("", AMPSQueueSemantics.NONE);
            
            String payload = "data";

            AMPSMessage message = buildMessage(payload);

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            doEmitRecord(emitter, message, output);

            assertEquals(1, output.getRecords().size(), "Should have emitted one record");
            assertEquals(payload, output.getRecords().get(0), "Should have the same payload");
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testEmitObject() throws Exception {
            AMPSRecordEmitter<AMPSPojo> emitter = buildEmitter(TestConstants.getAMPSPojoDeserializer(), "", AMPSQueueSemantics.NONE);

            AMPSPojo data = new AMPSPojo(1, "data", 1);

            SerializationSchema<AMPSPojo> schema = TestConstants.getAMPSPojoSerializer();
            schema.open(null);
            AMPSMessage message = new AMPSMessage(schema.serialize(data));
            message.setSubId(new Field("0"));
            message.setCommand(Message.Command.Publish);

            AMPSReaderOutput<AMPSPojo> output = new AMPSReaderOutput<>();

            doEmitRecord(emitter, message, output);

            assertEquals(1, output.getRecords().size(), "Should have emitted one record");
            assertEquals(data, output.getRecords().get(0), "Should have the same payload");
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testDiscardAfterEmitTrue() throws Exception {
            testDiscardAfterEmit(true, 0, "Should have discarded bookmark");
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testDiscardAfterEmitFalse() throws Exception {
            testDiscardAfterEmit(false, 1, "Should not have discarded bookmark");
        }

        private void testDiscardAfterEmit(boolean discardAfterEmit, int correctBookmarkCount, String assertMsg) throws Exception {
            DummyClient client = new DummyClient();
            DummyBookmarkStore bookmarkStore = new DummyBookmarkStore();
            client.setBookmarkStore(bookmarkStore);

            AMPSRecordEmitter<String> emitter = buildEmitter(AMPSQueueSemantics.NONE, client, discardAfterEmit);
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();
            
            bookmarkStore.log(null);
            
            AMPSMessage message = buildMessage("data");
            doEmitRecord(emitter, message, output);

            assertEquals(correctBookmarkCount, bookmarkStore.bookmarkCount, assertMsg);
        }
    }

    @Nested
    public class QueueSemanticsCheckpointing {
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testNone() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter("", AMPSQueueSemantics.NONE);

            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            AMPSMessage message = buildMessage("data");
            message.setSubId(new Field("1"));
            doEmitRecord(emitter, message, output);

            message = buildMessage("data2");
            message.setSubId(new Field("1"));
            doEmitRecord(emitter, message, output);

            assertEquals(0, emitter.getPendingMessages().size(), "Should have no pending messages");
            assertEquals(1, emitter.getLastEmittedMessages().size(), 
                "Should have one last emitted message due to same split index");

            emitter.snapshotPendingMessages(1L);

            assertEquals(1, emitter.getPendingMessagesByCheckpoint().get(1L).size(), 
                "Should have one message, which is the last emitted message");
            assertEquals(output.getRecords().get(1),
                emitter.getPendingMessagesByCheckpoint().get(1L).get(0).getData(),
                "Should have put the last emitted message in the map");
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testAtLeastOnce() throws Exception {
            AMPSRecordEmitter<String> emitter = buildEmitter("", AMPSQueueSemantics.AT_LEAST_ONCE);

            AMPSMessage message = buildMessage("data");
            
            AMPSReaderOutput<String> output = new AMPSReaderOutput<>();

            doEmitRecord(emitter, message, output);
            doEmitRecord(emitter, message, output);

            assertEquals(2, emitter.getPendingMessages().size(), "Should have added a pending message");

            emitter.snapshotPendingMessages(1L);

            assertEquals(0, emitter.getPendingMessages().size(), "Should have no pending messages after snapshot");
            assertEquals(2, emitter.getPendingMessagesByCheckpoint().get(1L).size(), 
                "Should have stored two messages in the first checkpoint");
        }
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void testAtMostOnce() throws Exception {
            DummyClient client = new DummyClient();

            AMPSRecordEmitter<String> emitter = buildEmitter(AMPSQueueSemantics.AT_MOST_ONCE, client, false);

            AMPSMessage message = buildMessage("data");

            doEmitRecord(emitter, message, new AMPSReaderOutput<>());

            assertEquals(1, client.acks, "Should have sent an ack");
        }
    }

    private static <T> void doEmitRecord(AMPSRecordEmitter<T> emitter, AMPSMessage message, AMPSReaderOutput<T> output) throws Exception {
        emitter.emitRecord(message, output, new AMPSSplitState("t", ""));
    }

    private static AMPSMessage buildMessage(String payload) {
        AMPSMessage message = new AMPSMessage();

        message.setData(payload);
        message.setBookmark("bm");
        message.setTopic("t");
        message.setSubId(new Field("0"));
        message.setCommand(Message.Command.Publish);

        return message;
    }

    private static AMPSRecordEmitter<String> buildEmitter(AMPSQueueSemantics semantics, HAClient client, boolean discardAfterEmit) {
        return new AMPSRecordEmitter<>(
            new AMPSSourceReaderContext(),
            "",
            semantics,
            client,
            discardAfterEmit,
            10_000L,
            new AMPSDataOnlyDeserializationSchemaWrapper<>(TestConstants.getStringSchema()));
    }

    private static AMPSRecordEmitter<String> buildEmitter(String options, AMPSQueueSemantics semantics) {
        return buildEmitter(
            TestConstants.getStringSchema(),
            options,
            semantics);
    }

    private static <T> AMPSRecordEmitter<T> buildEmitter(DeserializationSchema<T> deserializationSchema, String options, AMPSQueueSemantics semantics) {
        return new AMPSRecordEmitter<>(
            new AMPSSourceReaderContext(),
            options,
            semantics,
            new DummyClient(),
            false,
            10_000L,
            new AMPSDataOnlyDeserializationSchemaWrapper<>(deserializationSchema));
    }

    private static class DummyClient extends HAClient {
        public int acks = 0;

        @Override
        public void ack(byte[] topicBytes, int topicStart, int topicLength,
                        byte[] bookmark, int bookmarkStart, int bookmarkLength) {
            acks++;
        }
    }

    private static class DummyBookmarkStore extends DefaultBookmarkStore {
        public long bookmarkCount = 0;

        @Override
        public long log(Message message) {
            bookmarkCount++;
            return bookmarkCount;
        }

        @Override
        public void discard(Field subId, long bookmarkSeqNo) {
            bookmarkCount--;
        }
    }
}

