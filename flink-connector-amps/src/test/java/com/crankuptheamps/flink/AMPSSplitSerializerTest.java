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

import com.crankuptheamps.flink.source.split.AMPSSplit;
import com.crankuptheamps.flink.source.split.AMPSSplitSerializer;
import com.crankuptheamps.flink.testutils.TestConstants;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AMPSSplitSerializerTest {
    private static final AMPSSplit SPLIT = new AMPSSplit("splitSerializerTest", "/num MOD 2 = 0", "0");
    private static final byte[] V1_BYTES = {0, 19, 115, 112, 108, 105, 116, 83, 101, 114, 105, 97, 108, 105, 122, 101, 114, 84, 101, 115, 116, 0, 14, 47, 110, 117, 109, 32, 77, 79, 68, 32, 50, 32, 61, 32, 48, 0, 1, 48};

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testSerialize() throws Exception {
        AMPSSplitSerializer serializer = new AMPSSplitSerializer();

        byte[] serializedSplit = serializer.serialize(SPLIT);

        assertTrue(Arrays.equals(V1_BYTES, serializedSplit), "Should have serialized properly");
    }

    @Test
    @Timeout(value = TestConstants.SHORT_TIMEOUT, unit = TimeUnit.SECONDS)
    public void testDeserializeV1() throws Exception {
        AMPSSplitSerializer serializer = new AMPSSplitSerializer();

        AMPSSplit split = serializer.deserialize(1, V1_BYTES);

        assertEquals(SPLIT.getTopic(), split.getTopic(), "Should have same topic");
        assertEquals(SPLIT.getSplitFilter(), split.getSplitFilter(), "Should have same split filter");
        assertEquals(SPLIT.getBookmark(), split.getBookmark(), "Should have same bookmark");
    }
}

