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

package com.crankuptheamps.flink.source.split;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A serializer for {@link AMPSSplit}.
 */
@Internal
public class AMPSSplitSerializer implements SimpleVersionedSerializer<AMPSSplit> {

    private static final int VERSION = 1;

    /**
     * Returns the version of the serializer.
     *
     * @return The version of the serializer.
     */
    @Override
    public int getVersion() {
        return VERSION;
    }

    /**
     * Serializes a {@link AMPSSplit} into a byte array.
     *
     * @param split The split to serialize.
     * @return The serialized split.
     * @throws IOException if any error occurs during serialization.
     */
    @Override
    public byte[] serialize(AMPSSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getTopic());
            out.writeUTF(split.getSplitFilter());
            out.writeUTF(split.getBookmark());
            return baos.toByteArray();
        }
    }

    /**
     * Deserializes a {@link AMPSSplit} from a byte array.
     *
     * @param version    The version of the serializer.
     * @param serialized The serialized split.
     * @return The deserialized split.
     * @throws IOException if any error occurs during deserialization.
     */
    @Override
    public AMPSSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            String topic = in.readUTF();
            String splitFilter = in.readUTF();
            String bookmark = in.readUTF();
            return new AMPSSplit(topic, splitFilter, bookmark);
        }
    }
}
