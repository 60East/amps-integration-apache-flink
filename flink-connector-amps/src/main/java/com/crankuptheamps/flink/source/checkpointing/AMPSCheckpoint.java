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

import java.util.ArrayList;
import java.util.List;

import com.crankuptheamps.flink.source.split.AMPSSplit;

import org.apache.flink.annotation.Internal;

/**
 * Represents a checkpoint for the {@link com.crankuptheamps.flink.source.split.AMPSSplitEnumerator}. 
 *
 * It stores the count of assigned readers.
 */
@Internal
public class AMPSCheckpoint {

    /** The count of assigned readers. */
    private final int readers;
    /** A list of unassigned splits. */
    private final ArrayList<AMPSSplit> unassignedSplits;

    /**
     * Creates a new {@link AMPSCheckpoint}.
     *
     * @param readers The amount of readers that the enumerator assigned.
     */
    public AMPSCheckpoint(int readers, List<AMPSSplit> unassignedSplits) {
        this.readers = readers;
        this.unassignedSplits = new ArrayList<>(unassignedSplits);
    }

    /**
     * Returns the readers of this checkpoint.
     *
     * @return The amount of assigned readers.
     */
    public int getReaders() {
        return readers;
    }

    /**
     * Returns the unassigned splits of this checkpoint.
     *
     * @return The unassigned splits.
     */
    public ArrayList<AMPSSplit> getUnassignedSplits() {
        return unassignedSplits;
    }
}

