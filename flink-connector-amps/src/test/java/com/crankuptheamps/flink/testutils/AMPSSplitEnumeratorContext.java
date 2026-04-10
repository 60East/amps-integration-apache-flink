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

package com.crankuptheamps.flink.testutils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import com.crankuptheamps.flink.source.split.AMPSSplit;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

public class AMPSSplitEnumeratorContext implements SplitEnumeratorContext<AMPSSplit> {
    public final int parallelism;
    public final Map<Integer, Integer> splitsPerSubtask = new HashMap<>();

    public AMPSSplitEnumeratorContext() {
        parallelism = 1;
    }

    public AMPSSplitEnumeratorContext(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public void assignSplit(AMPSSplit split, int subtask) {
        splitsPerSubtask.put(subtask, 1);
    }

    @Override
    public void assignSplits(SplitsAssignment<AMPSSplit> newSplitAssignments) {
        Map<Integer, List<AMPSSplit>> assignments = newSplitAssignments.assignment();

        for (Map.Entry<Integer, List<AMPSSplit>> entry : assignments.entrySet()) {
            splitsPerSubtask.put(entry.getKey(), entry.getValue().size());
        }
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelayMillis, long periodMillis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIsProcessingBacklog(boolean isProcessingBacklog) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        splitsPerSubtask.put(subtask, 0);
    }
}

