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
import java.util.concurrent.TimeUnit;

import com.crankuptheamps.flink.sink.writer.AMPSWriterState;
import com.crankuptheamps.flink.sink.writer.AMPSWriterStateSerializer;
import com.crankuptheamps.flink.testutils.TestConstants;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSWriterStateSerializerTest {
    private static final AMPSWriterState STATE = new AMPSWriterState(1);
    private static final byte[] V1_BYTES = {0, 0, 0, 0, 0, 0, 0, 1};

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testSerialize() throws Exception {
        AMPSWriterStateSerializer serializer = new AMPSWriterStateSerializer();

        byte[] serializedState = serializer.serialize(STATE);

        assertTrue(Arrays.equals(V1_BYTES, serializedState), "Should have serialized properly");
    }

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testDeserializeV1() throws Exception {
        AMPSWriterStateSerializer serializer = new AMPSWriterStateSerializer();

        AMPSWriterState state = serializer.deserialize(1, V1_BYTES);

        assertEquals(STATE.getLastTimestamp(), state.getLastTimestamp(), "Should have same last timestamp");
    }
}

