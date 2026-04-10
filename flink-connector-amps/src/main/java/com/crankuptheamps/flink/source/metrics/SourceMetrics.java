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

package com.crankuptheamps.flink.source.metrics;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The metrics that an {@link com.crankuptheamps.flink.source.AMPSSource} exposes.
 */
@PublicEvolving
public class SourceMetrics {

    /** Groups for the metrics. */
    public static class Group {
        public static final String SOURCE = "amps_source";
        public static final String READER = "reader";
    }

    /** Metrics exposed. */
    public static class Metric {
        /** The number of messages in the internal buffer of a client. */
        public static final String USED_QUEUE_CAPACITY = "used_queue_capacity";
        /** The remaining capacity of the internal buffer of a client. */
        public static final String REMAINING_QUEUE_CAPACITY = "remaining_queue_capacity";
        /** The amount of connects from an AMPSSource. */
        public static final String CONNECTS = "connects";
        /** The amount of disconnects from an AMPSSource. */
        public static final String DISCONNECTS = "disconnects";
    }
}

