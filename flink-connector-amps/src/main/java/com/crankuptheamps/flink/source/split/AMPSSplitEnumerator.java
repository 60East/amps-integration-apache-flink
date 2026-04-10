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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.crankuptheamps.flink.source.checkpointing.AMPSCheckpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SplitEnumerator} for the {@link com.crankuptheamps.flink.source.AMPSSource}.
 *
 * It is responsible for discovering the splits (subscriptions) and 
 * assigning them to the readers.
 */
@Internal
public class AMPSSplitEnumerator implements SplitEnumerator<AMPSSplit, AMPSCheckpoint> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMPSSplitEnumerator.class);

    /** The context for the split enumerator. */
    private final SplitEnumeratorContext<AMPSSplit> context;
    /** The splits that will be assigned to the readers. */
    private final LinkedList<AMPSSplit> splits = new LinkedList<>();
    
    /** The amount of readers that were assigned a split. Used to help determine how many splits a reader should be assigned. */
    private int assignedReaders = 0;

    /**
     * Creates a new split enumerator.
     *
     * @param context   The context for the enumerator.
     * @param splits    The splits to assign the readers.
     */
    public AMPSSplitEnumerator(SplitEnumeratorContext<AMPSSplit> context, Collection<AMPSSplit> splits) {
        this.context = context;

        if (!splits.isEmpty()) {
            LOGGER.info("Set {} split(s)", splits.size());
            this.splits.addAll(splits);
        } else {
            LOGGER.warn("Received no splits");
        }
    }

    /**
     * Restores a split enumerator.
     *
     * @param context       The context for this enumerator.
     * @param checkpoint    The checkpoint to resume from.
     */
    public AMPSSplitEnumerator(SplitEnumeratorContext<AMPSSplit> context, AMPSCheckpoint checkpoint) {
        this.context = context;
        this.assignedReaders = checkpoint.getReaders();
        this.splits.addAll(checkpoint.getUnassignedSplits());
    }

    /**
     * Starts the split enumerator.
     */
    @Override
    public void start() {
    }

    /**
     * Handles a split request from a source reader.
     *
     * @param subtaskId         The subtask ID of the requesting reader.
     * @param requesterHostname The hostname of the requesting reader.
     */
    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        LOGGER.info("Received split request from subtask {}", subtaskId);
        assignSplitsToReader(subtaskId); 
    }

    /**
     * Adds splits back to the enumerator.
     *
     * @param splits    The splits to add back.
     * @param subtaskId The subtask ID of the reader that returned the splits.
     */
    @Override
    public void addSplitsBack(List<AMPSSplit> splits, int subtaskId) {
        LOGGER.info("Add {} split(s) back", splits.size());
        this.splits.addAll(splits);
        assignedReaders--;
    }

    /**
     * Adds a new reader to the enumerator.
     *
     * @param subtaskId The subtask ID of the new reader.
     */
    @Override
    public void addReader(int subtaskId) {
        LOGGER.info("Adding reader from subtask {}", subtaskId);
        assignSplitsToReader(subtaskId);    
    }

    /**
     * Snapshots the state of the enumerator.
     *
     * @param checkpointId The ID of the checkpoint.
     * @return The checkpoint of the enumerator.
     * @throws Exception if any error occurs during snapshotting.
     */
    @Override
    public AMPSCheckpoint snapshotState(long checkpointId) throws Exception {
       return new AMPSCheckpoint(assignedReaders, splits);
    }

    /**
     * Closes the enumerator.
     */
    @Override
    public void close() {
    }

    /**
     * Assigns split(s) to a reader with the given subtaskId.
     *
     * @param subtaskId The id of the reader that should be assigned splits.
     */
    private void assignSplitsToReader(int subtaskId) {
        int readers = context.currentParallelism() - assignedReaders;
        
        if (readers <= 0) {
            // Should only assign a maximum of readers equal to the current parallelism
            LOGGER.debug("Already assigned {} readers", context.currentParallelism());
            context.signalNoMoreSplits(subtaskId);
            return;
        }

        int remainingSplits = splits.size();
        int splitsToAssignCount = (int) Math.ceil((double) remainingSplits / readers);
       
        if (splitsToAssignCount == 1) {
            LOGGER.debug("Assigning 1 split: {}", splits.peek());
            context.assignSplit(splits.poll(), subtaskId);
            assignedReaders++;
        } else if (splitsToAssignCount > 1) {
            // Parallelism is lower than split count, so readers may get multiple splits
            List<AMPSSplit> splitsToAssign = new LinkedList<>();

            while (splitsToAssignCount > 0 && !splits.isEmpty()) {
                splitsToAssign.add(splits.poll());
                splitsToAssignCount--;
            }

            Map<Integer, List<AMPSSplit>> splitMap = new HashMap<>();

            splitMap.put(subtaskId, splitsToAssign);

            LOGGER.debug("Assigning splits: {}", splitsToAssign);
            context.assignSplits(new SplitsAssignment<>(splitMap));
            assignedReaders++;
        } else {
            // Parallelism is higher than split count, so extra readers should finish and close
            LOGGER.debug("No more splits");
            context.signalNoMoreSplits(subtaskId);
        }
    }
}

