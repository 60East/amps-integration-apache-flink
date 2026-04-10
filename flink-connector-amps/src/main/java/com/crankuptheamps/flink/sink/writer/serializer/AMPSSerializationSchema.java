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

package com.crankuptheamps.flink.sink.writer.serializer;

import java.io.Serializable;

import com.crankuptheamps.flink.util.SerializedElement;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 *  An interface for serializing data before publishing to AMPS.
 *
 *  @param <IN> The input type of the schema.
 */
@PublicEvolving
public interface AMPSSerializationSchema<IN> extends Serializable {
    
    /**
     * Initialization method for the schema.
     *
     * Called once before {@link #serialize(IN, SinkWriter.Context)}
     * and can be used for setup work.
     *
     * @param context Context for initialization.
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes the given element into a {@link SerializedElement}.
     *
     * @param element The element to serialize.
     * @param context The {@link SinkWriter.Context} to provide extra information.
     * @return The serialized element.
     */
    SerializedElement serialize(IN element, SinkWriter.Context context);
}

