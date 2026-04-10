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

import com.crankuptheamps.client.Message;
import com.crankuptheamps.flink.util.AMPSMessage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

/**
 *  A class for deserializing an {@link AMPSMessage} using a {@link DeserializationSchema}.
 *
 *  This class is used as a wrapper for an existing {@link DeserializationSchema}. It will only emit
 *  the data from {@link AMPSMessage#getDataRaw()} if the message has the command publish, oof, or sow.
 *
 *  @param <OUT> The output type of the schema.
 */
@PublicEvolving
public class AMPSDataOnlyDeserializationSchemaWrapper<OUT> implements AMPSDeserializationSchema<OUT> {
    
    private static final long serialVersionUID = 1L;
    /** The commands to allow messages to be deserialized. */
    private static final int filterCommand = Message.Command.DeltaPublish | 
                                                Message.Command.Publish |
                                                Message.Command.OOF |
                                                Message.Command.SOW;

    /** Schema to deserialize the data from an {@link AMPSMessage}. */
    private final DeserializationSchema<OUT> deserializationSchema;

    public AMPSDataOnlyDeserializationSchemaWrapper(DeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        deserializationSchema.open(context);
    }

    @Override
    public void deserialize(AMPSMessage message, Collector<OUT> out) throws IOException {
        // Should not emit and deserialize from commands that are not included in the filter.
        if ((message.getCommand() & filterCommand) == 0) return;

        out.collect(deserializationSchema.deserialize(message.getDataRaw()));
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}

