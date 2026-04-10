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

package com.crankuptheamps.flink.util;

import java.util.EnumSet;
import java.util.Set;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Enum used in the {@link com.crankuptheamps.flink.source.AMPSSource} for determining what parts
 * of a {@link com.crankuptheamps.client.Message} to preserve in an {@link com.crankuptheamps.flink.util.AMPSMessage}.
 */
@PublicEvolving
public enum AMPSSourceHeaderKeys {
    
    /** Header key for the message command type. */
    COMMAND,

    /** Header key for the message topic. */
    TOPIC,

    /** Header key for the SOW key for the message. */
    SOW_KEY,

    /** Header key for the message's ISO-8601 timestamp of when it was processed by the AMPS server. */
    TIMESTAMP,

    /** Header key for the message bookmark. */
    BOOKMARK,

    /** Header key for the message correlation ID. */
    CORRELATION_ID,

    /** Header key for the message's subscription ID. */
    SUBSCRIPTION_ID,

    /** Header key for the message body length in bytes. */
    LENGTH;
} 

