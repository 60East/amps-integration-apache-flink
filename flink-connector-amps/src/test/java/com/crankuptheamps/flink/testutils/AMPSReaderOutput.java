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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;

public class AMPSReaderOutput<T> implements ReaderOutput<T> {
    private final List<T> records = new ArrayList<>();
    private final List<Long> timestamps = new ArrayList<>();

    @Override
    public void collect(T record) {
        records.add(record);
    }

    @Override
    public void collect(T record, long timestamp) {
        collect(record);
        timestamps.add(timestamp);
    }

    @Override
    public SourceOutput<T> createOutputForSplit(String splitId) {
        return new SourceOutput<T>() {
            private final List<T> r = records;
            private final List<Long> ts = timestamps;

            @Override
            public void collect(T record) {
                r.add(record);
            }

            @Override
            public void collect(T record, long timestamp) {
                collect(record);
                ts.add(timestamp);
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void markIdle() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void markActive() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override 
    public void emitWatermark(Watermark watermark) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markIdle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
        // no-op
    }

    @Override
    public void markActive() {
        throw new UnsupportedOperationException();
    }

    public List<T> getRecords() {
        return records;
    }

    public List<Long> getTimestamps() {
        return timestamps;
    }
}
