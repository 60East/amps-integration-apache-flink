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

package com.crankuptheamps.flink.source.reader.deserializer;

import java.io.IOException;
import java.io.Serializable;

import com.crankuptheamps.flink.util.AMPSMessage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

/**
 *  An interface for deserializing an {@link AMPSMessage}.
 *
 *  @param <OUT> The output type of the schema.
 */
@PublicEvolving
public interface AMPSDeserializationSchema<OUT> extends Serializable, ResultTypeQueryable<OUT> {
    
    /**
     * Initialization method for the schema.
     *
     * Called once before {@link #deserialize(AMPSMessage, Collector<OUT>)}
     * and can be used for setup work.
     *
     * @param context Context for initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes the {@link AMPSMessage} and outputs the deserialized data to the {@link Collector}.
     *
     * @param message The message to deserialize.
     * @param out The collector to output the deserialized record.
     */
    void deserialize(AMPSMessage message, Collector<OUT> out) throws IOException;
}

