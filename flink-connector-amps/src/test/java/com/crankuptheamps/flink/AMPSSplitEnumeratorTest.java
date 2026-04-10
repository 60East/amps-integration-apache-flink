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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.flink.source.checkpointing.AMPSCheckpoint;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.source.split.AMPSSplitEnumerator;
import com.crankuptheamps.flink.testutils.AMPSSplitEnumeratorContext;
import com.crankuptheamps.flink.testutils.TestConstants;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AMPSSplitEnumeratorTest {
    private static final String TOPIC = "splitEnumeratorTest";

    @Nested
    public class AddReaderTest {
        
        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void oneSplitOneReader() throws Exception {
            final int parallelism = 1;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.addReader(101);

                assertEquals(1, context.splitsPerSubtask.size(), "Should have assigned splits to a subtask");
                assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void twoSplitsOneReader() throws Exception {
            final int parallelism = 1;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.addReader(101);

                assertEquals(1, context.splitsPerSubtask.size(), "Should have assigned splits to a subtask");
                assertEquals(2, context.splitsPerSubtask.get(101), "Should have assigned two splits to subtask 101");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void twoSplitsTwoReaders() throws Exception {
            final int parallelism = 2;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.addReader(101);
                splitEnum.addReader(102);

                assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
                assertEquals(1, context.splitsPerSubtask.get(102), "Should have assigned one split to subtask 102");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void oneSplitTwoReaders() throws Exception {
            final int parallelism = 2;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.addReader(101);
                splitEnum.addReader(102);

                assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
                assertEquals(0, context.splitsPerSubtask.get(102), "Should have assigned no splits to subtask 102");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void threeSplitsTwoReaders() throws Exception {
            final int parallelism = 2;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));
            
            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.addReader(101);
                splitEnum.addReader(102);

                assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(2, context.splitsPerSubtask.get(101), "Should have assigned two splits to subtask 101");
                assertEquals(1, context.splitsPerSubtask.get(102), "Should have assigned one split to subtask 102");
            }
        }
    }

    @Nested
    public class HandleSplitRequestTest {

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void oneSplitOneReader() throws Exception {
            final int parallelism = 1;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.handleSplitRequest(101, "");

                assertEquals(1, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void twoSplitsOneReader() throws Exception {
            final int parallelism = 1;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.handleSplitRequest(101, "");

                assertEquals(1, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(2, context.splitsPerSubtask.get(101), "Should have assigned two splits to subtask 101");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void twoSplitsTwoReaders() throws Exception {
            final int parallelism = 2;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.handleSplitRequest(101, "");
                splitEnum.handleSplitRequest(102, "");

                assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
                assertEquals(1, context.splitsPerSubtask.get(102), "Should have assigned one split to subtask 102");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void oneSplitTwoReaders() throws Exception {
            final int parallelism = 2;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.handleSplitRequest(101, "");
                splitEnum.handleSplitRequest(102, "");

                assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
                assertEquals(0, context.splitsPerSubtask.get(102), "Should have assigned no splits to subtask 102");
            }
        }

        @Test
        @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
        public void threeSplitsTwoReaders() throws Exception {
            final int parallelism = 2;
            AMPSSplitEnumeratorContext context = makeContext(parallelism);

            List<AMPSSplit> splits = new ArrayList<>();
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));
            splits.add(new AMPSSplit(TOPIC, ""));

            try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
                splitEnum.start();

                splitEnum.handleSplitRequest(101, "");
                splitEnum.handleSplitRequest(102, "");

                assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
                assertEquals(2, context.splitsPerSubtask.get(101), "Should have assigned two splits to subtask 101");
                assertEquals(1, context.splitsPerSubtask.get(102), "Should have assigned one split to subtask 102");
            }
        }
    }

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testAddSplitsBack() throws Exception {
        final int parallelism = 3;
        AMPSSplitEnumeratorContext context = makeContext(parallelism);

        List<AMPSSplit> splits = new ArrayList<>();
        splits.add(new AMPSSplit(TOPIC, ""));

        try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
            splitEnum.start();

            splitEnum.addReader(101);
            splitEnum.addReader(102);

            assertEquals(2, context.splitsPerSubtask.size(), "Should have assigned splits to subtasks");
            assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned one split to subtask 101");
            assertEquals(0, context.splitsPerSubtask.get(102), "Should have assigned no splits to subtask 102");

            splitEnum.addSplitsBack(splits, 101);
            splitEnum.addReader(103);

            assertEquals(3, context.splitsPerSubtask.size(), "Should have assigned splits after adding new reader");
            assertEquals(1, context.splitsPerSubtask.get(103), "Should have assigned one split to subtask 103");
        }
    }

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testSnapshotState() throws Exception {
        final int parallelism = 2;
        AMPSSplitEnumeratorContext context = makeContext(parallelism);

        List<AMPSSplit> splits = new ArrayList<>();
        splits.add(new AMPSSplit(TOPIC, ""));
        splits.add(new AMPSSplit(TOPIC, ""));

        try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
            splitEnum.start();

            splitEnum.addReader(101);
            AMPSCheckpoint c1 = splitEnum.snapshotState(1);

            assertEquals(1, c1.getReaders(), "Should have snapshotted one current reader");

            splitEnum.addReader(102);
            AMPSCheckpoint c2 = splitEnum.snapshotState(2);

            assertEquals(2, c2.getReaders(), "Should have snapshotted two current readers");
        }
    }
    
    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testRestoreFromCheckpointAllSplitsAssigned() throws Exception {
        final int parallelism = 1;
        AMPSSplitEnumeratorContext context = makeContext(parallelism);
        AMPSSplitEnumeratorContext restoredContext = makeContext(parallelism);

        List<AMPSSplit> splits = new ArrayList<>();
        splits.add(new AMPSSplit(TOPIC, ""));

        AMPSSplitEnumerator restoredEnum = null;

        try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
            splitEnum.start();

            splitEnum.addReader(101);
            AMPSCheckpoint c1 = splitEnum.snapshotState(1);

            restoredEnum = new AMPSSplitEnumerator(restoredContext, c1);

            splitEnum.addReader(102);
            restoredEnum.addReader(102);

            // Neither split enumerator should assign a split
            assertEquals(0, context.splitsPerSubtask.get(102), "Should have assigned no splits to subtask 102");
            assertEquals(0, restoredContext.splitsPerSubtask.get(102), "Should have assigned no splits to subtask 102 after restoring");
        } finally {
            if (restoredEnum != null) {
                restoredEnum.close();
            }
        }
    }

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testRestoreFromCheckpointSomeSplitsAssigned() throws Exception {
        final int parallelism = 2;
        AMPSSplitEnumeratorContext context = makeContext(parallelism);
        AMPSSplitEnumeratorContext restoredContext = makeContext(parallelism);

        List<AMPSSplit> splits = new ArrayList<>();
        splits.add(new AMPSSplit(TOPIC, ""));
        splits.add(new AMPSSplit(TOPIC, ""));

        AMPSSplitEnumerator restoredEnum = null;

        try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
            splitEnum.start();

            splitEnum.addReader(101);
            AMPSCheckpoint c1 = splitEnum.snapshotState(1);

            restoredEnum = new AMPSSplitEnumerator(restoredContext, c1);

            splitEnum.addReader(102);
            restoredEnum.addReader(102);

            // Both split enumerators should be capable of assigning the remaining split
            assertEquals(1, context.splitsPerSubtask.get(102), "Should have assigned remaining split to subtask 102");
            assertEquals(1, restoredContext.splitsPerSubtask.get(102), "Should have assigned remaining split to subtask 102 after restoring");
        } finally {
            if (restoredEnum != null) {
                restoredEnum.close();
            }
        }
    }
    
    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testRestoreFromCheckpointNoSplitsAssigned() throws Exception {
        final int parallelism = 2;
        AMPSSplitEnumeratorContext context = makeContext(parallelism);
        AMPSSplitEnumeratorContext restoredContext = makeContext(parallelism);

        List<AMPSSplit> splits = new ArrayList<>();
        splits.add(new AMPSSplit(TOPIC, ""));
        splits.add(new AMPSSplit(TOPIC, ""));

        AMPSSplitEnumerator restoredEnum = null;

        try (AMPSSplitEnumerator splitEnum = new AMPSSplitEnumerator(context, splits);) {
            splitEnum.start();

            AMPSCheckpoint c1 = splitEnum.snapshotState(1);

            restoredEnum = new AMPSSplitEnumerator(restoredContext, c1);
            
            splitEnum.addReader(101);
            restoredEnum.addReader(101);

            splitEnum.addReader(102);
            restoredEnum.addReader(102);

            // Both split enumerators should be capable of assigning the remaining split
            assertEquals(1, context.splitsPerSubtask.get(101), "Should have assigned a split to subtask 101");
            assertEquals(1, restoredContext.splitsPerSubtask.get(101), "Should have assigned a split to subtask 101 after restoring");
            
            assertEquals(1, context.splitsPerSubtask.get(102), "Should have assigned a split to subtask 102");
            assertEquals(1, restoredContext.splitsPerSubtask.get(102), "Should have assigned a split to subtask 102 after restoring");
        } finally {
            if (restoredEnum != null) {
                restoredEnum.close();
            }
        }
    }

    private AMPSSplitEnumeratorContext makeContext(int parallelism) {
        return new AMPSSplitEnumeratorContext(parallelism);
    }
}

