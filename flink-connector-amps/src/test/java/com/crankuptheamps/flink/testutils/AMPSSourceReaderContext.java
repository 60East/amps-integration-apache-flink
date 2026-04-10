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

import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

public class AMPSSourceReaderContext implements SourceReaderContext {
    public final AMPSSourceReaderMetricGroup metricGroup = new AMPSSourceReaderMetricGroup();
    public final int parallelism;
    public int splitRequestsSent = 0;

    public AMPSSourceReaderContext() {
        this.parallelism = 1;
    }

    public AMPSSourceReaderContext(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration();
    }

    @Override
    public int getIndexOfSubtask() {
        return 1;
    }

    @Override
    public String getLocalHostName() {
        return "testHost";
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
    }

    @Override
    public SourceReaderMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendSplitRequest() {
        splitRequestsSent++;
    }
}

