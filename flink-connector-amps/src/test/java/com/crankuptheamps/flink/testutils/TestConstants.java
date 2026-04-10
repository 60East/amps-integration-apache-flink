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

package com.crankuptheamps.flink.testutils;

import java.nio.charset.StandardCharsets;

import com.crankuptheamps.client.Client;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;

public class TestConstants {
    // Main connection string
    public static final String URI = "tcp://localhost:9080/amps/json";
   
    // SSL connection string
    public static final String S_URI = "tcps://localhost:9085/amps/json";

    // Connection string to a transport that will be disabled
    public static final String SOURCE_DISABLE_URI = "tcp://localhost:9083/amps/json"; 
    public static final String SINK_DISABLE_URI = "tcp://localhost:9084/amps/json";

    // URL to test if the AMPS instance is running
    public static final String URL = "http://localhost:9082/";

    // Timeouts for the tests in seconds
    public static final int LONG_TIMEOUT = 10;
    public static final int NORMAL_TIMEOUT = 5;
    public static final int SHORT_TIMEOUT = 2;

    public static SimpleStringSchema getStringSchema() {
        return new SimpleStringSchema(StandardCharsets.UTF_8);
    }

    public static JsonDeserializationSchema<AMPSPojo> getAMPSPojoDeserializer() {
        return new JsonDeserializationSchema<>(AMPSPojo.class);
    }

    public static JsonSerializationSchema<AMPSPojo> getAMPSPojoSerializer() {
        return new JsonSerializationSchema<>();
    }

    public static void sowDelete(Client client, String topic) throws Exception {
        client.sowDelete(topic,
            "1=1",
            (SHORT_TIMEOUT - 1) * 1000);
    }
}

