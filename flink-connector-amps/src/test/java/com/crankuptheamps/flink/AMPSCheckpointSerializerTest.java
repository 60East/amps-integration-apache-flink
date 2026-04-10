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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.flink.source.checkpointing.AMPSCheckpoint;
import com.crankuptheamps.flink.source.checkpointing.AMPSCheckpointSerializer;
import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.testutils.TestConstants;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSCheckpointSerializerTest {
    private static final List<AMPSSplit> SPLITS = List.of(new AMPSSplit("AMPSCheckpointSerializerTest", ""));
    private static final AMPSCheckpoint CHECKPOINT = new AMPSCheckpoint(3, SPLITS);
    private static final byte[] V1_BYTES = {0, 0, 0, 3};
    private static final byte[] V2_BYTES = {-84, -19, 0, 5, 119, 4, 0, 0, 0, 3, 115, 114, 0, 19, 106, 97, 118,
        97, 46, 117, 116, 105, 108, 46, 65, 114, 114, 97, 121, 76, 105, 115, 116, 120, -127, -46, 29, -103, -57,
        97, -99, 3, 0, 1, 73, 0, 4, 115, 105, 122, 101, 120, 112, 0, 0, 0, 1, 119, 4, 0, 0, 0, 1, 115, 114, 0, 47,
        99, 111, 109, 46, 99, 114, 97, 110, 107, 117, 112, 116, 104, 101, 97, 109, 112, 115, 46, 102, 108, 105, 110,
        107, 46, 115, 111, 117, 114, 99, 101, 46, 115, 112, 108, 105, 116, 46, 65, 77, 80, 83, 83, 112, 108, 105, 116,
        0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 3, 76, 0, 8, 98, 111, 111, 107, 109, 97, 114, 107, 116, 0, 18, 76, 106, 97, 118,
        97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 76, 0, 11, 115, 112, 108, 105, 116, 70, 105,
        108, 116, 101, 114, 113, 0, 126, 0, 3, 76, 0, 5, 116, 111, 112, 105, 99, 113, 0, 126, 0, 3, 120, 112, 116, 0,
        0, 113, 0, 126, 0, 5, 116, 0, 28, 65, 77, 80, 83, 67, 104, 101, 99, 107, 112, 111, 105, 110, 116, 83, 101,
        114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 120};

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testSerialize() throws Exception {
        AMPSCheckpointSerializer serializer = new AMPSCheckpointSerializer();

        byte[] serializedCheckpoint = serializer.serialize(CHECKPOINT);

        assertTrue(Arrays.equals(V2_BYTES, serializedCheckpoint), "Should have serialized properly");
    }

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testDeserializeV1() throws Exception {
        AMPSCheckpointSerializer serializer = new AMPSCheckpointSerializer();

        AMPSCheckpoint checkpoint = serializer.deserialize(1, V1_BYTES);

        assertEquals(CHECKPOINT.getReaders(), checkpoint.getReaders(), "Should have same amount of readers");
    }
    
    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testDeserializeV2() throws Exception {
        AMPSCheckpointSerializer serializer = new AMPSCheckpointSerializer();

        AMPSCheckpoint checkpoint = serializer.deserialize(2, V2_BYTES);

        assertEquals(CHECKPOINT.getReaders(), checkpoint.getReaders(), "Should have same amount of readers");

        List<AMPSSplit> expected = CHECKPOINT.getUnassignedSplits();
        List<AMPSSplit> actual = checkpoint.getUnassignedSplits();

        assertEquals(expected.size(), actual.size(), "Should have same amount of splits");
        assertEquals(expected.get(0).toString(), actual.get(0).toString(), "Should have the same split");
    }
}

