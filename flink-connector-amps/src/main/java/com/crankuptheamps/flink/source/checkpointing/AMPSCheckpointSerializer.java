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

package com.crankuptheamps.flink.source.checkpointing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.crankuptheamps.flink.source.split.AMPSSplit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A serializer for {@link AMPSCheckpoint}.
 */
@Internal
public class AMPSCheckpointSerializer implements SimpleVersionedSerializer<AMPSCheckpoint> {

    private static final int VERSION = 2;

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
     * Serializes a {@link AMPSCheckpoint} into a byte array.
     *
     * @param checkpoint The checkpoint to serialize.
     * @return The serialized checkpoint.
     * @throws IOException if any error occurs during serialization.
     */
    @Override
    public byte[] serialize(AMPSCheckpoint checkpoint) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeInt(checkpoint.getReaders());
            out.writeObject(checkpoint.getUnassignedSplits());
            return baos.toByteArray();
        }
    }

    /**
     * Deserializes a {@link AMPSCheckpoint} from a byte array.
     *
     * @param version    The version of the serializer.
     * @param serialized The serialized checkpoint.
     * @return The deserialized checkpoint.
     * @throws IOException if any error occurs during deserialization.
     */
    @Override
    public AMPSCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                     DataInputStream in = new DataInputStream(bais)) {
                    int readers = in.readInt();
                    return new AMPSCheckpoint(readers, new ArrayList<>());
                }
            case 2:
                try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                     ObjectInputStream in = new ObjectInputStream(bais)) {
                    int readers = in.readInt();
                    @SuppressWarnings("unchecked")
                    ArrayList<AMPSSplit> unassignedSplits = (ArrayList<AMPSSplit>) in.readObject();
                    return new AMPSCheckpoint(readers, unassignedSplits);
                } catch (ClassNotFoundException cnfe) {
                    throw new IOException(cnfe);
                }
            default:
                throw new IOException("Unknown version: " + version);
        }
    }
}

