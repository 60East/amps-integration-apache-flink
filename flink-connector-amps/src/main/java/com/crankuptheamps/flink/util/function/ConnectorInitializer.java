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

package com.crankuptheamps.flink.util.function;

import java.io.Serializable;

import com.crankuptheamps.client.HAClient;

import org.apache.flink.annotation.Public;

/**
 * Functional interface that is called before an {@link HAClient}
 * from an {@link com.crankuptheamps.flink.source.reader.AMPSSourceReader} or an
 * {@link com.crankuptheamps.flink.sink.writer.AMPSStatefulSinkWriter} connects to AMPS.
 *
 * <p>
 * Can be used to make any final changes to the client before the Flink job starts.
 */
@Public
@FunctionalInterface
public interface ConnectorInitializer extends Serializable {
    public static final long serialVersionUID = 1L;

    /**
     * Called before an {@link HAClient} from a reader or writer connects to AMPS.
     * 
     * <p>
     * This method can connect the client to AMPS. The client will be set up based on the fields 
     * set in the builder, so modifying the client may lead to unexpected behavior.
     * 
     * <p>
     * Can be used to set up a keystore or truststore for
     * a {@link com.crankuptheamps.client.TCPSTransport} or set global properties.
     *
     * @param client The HAClient that the reader/writer will use.
     */
    void init(HAClient client) throws Exception;
}

