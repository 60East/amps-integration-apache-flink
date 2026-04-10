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
import java.util.Map;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

public class AMPSSourceReaderMetricGroup implements SourceReaderMetricGroup, OperatorIOMetricGroup {
    public final Map<String, Metric> metrics = new HashMap<>();
    public String currentNewGroup = "";

    public AMPSSourceReaderMetricGroup() {
        metrics.put("numRecordsInErrors", new SimpleCounter());
        metrics.put("numBytesIn", new SimpleCounter());
        metrics.put("numBytesOut", new SimpleCounter());
        metrics.put("numRecordsIn", new SimpleCounter());
        metrics.put("numRecordsOut", new SimpleCounter());
    }

    @Override
    public MetricGroup addGroup(String name) {
        currentNewGroup += name;
        return this;
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        return this;    
    }

    @Override
    public Counter counter(String name) {
        Counter counter = new SimpleCounter();
        currentNewGroup += name;
        metrics.put(currentNewGroup, counter);
        currentNewGroup = "";
        return counter;
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        currentNewGroup += name;
        metrics.put(currentNewGroup, counter);
        currentNewGroup = "";
        return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        currentNewGroup += name;
        metrics.put(currentNewGroup, gauge);
        currentNewGroup = "";
        return gauge;
    }

    @Override
    public Map<String, String> getAllVariables() {
        Map<String, String> vars = new HashMap<>();
        for (String key : metrics.keySet()) {
            vars.put(key, metrics.get(key).toString());
        }
        return vars;
    }

    @Override
    public String getMetricIdentifier(String metricName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getScopeComponents() {
        String[] strs = {""};
        return strs;
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
        currentNewGroup += name;
        metrics.put(currentNewGroup, histogram);
        currentNewGroup = "";
        return histogram;
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        currentNewGroup += name;
        metrics.put(currentNewGroup, meter);
        currentNewGroup = "";
        return meter;
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return this;
    }

    @Override
    public Counter getNumRecordsInErrorsCounter() {
        return (Counter) metrics.get("numRecordsInErrors");
    }

    @Override
    public void setPendingBytesGauge(Gauge<Long> pendingBytesGauge) {
        metrics.put("pendingBytes", pendingBytesGauge);
    }

    @Override
    public void setPendingRecordsGauge(Gauge<Long> pendingRecordsGauge) {
        metrics.put("pendingRecords", pendingRecordsGauge);
    }

    @Override
    public Counter getNumBytesInCounter() {
        return (Counter) metrics.get("numBytesIn");
    }

    @Override
    public Counter getNumBytesOutCounter() {
        return (Counter) metrics.get("numBytesOut");
    }

    @Override
    public Counter getNumRecordsInCounter() {
        return (Counter) metrics.get("numRecordsIn");
    }

    @Override
    public Counter getNumRecordsOutCounter() {
        return (Counter) metrics.get("numRecordsOut");
    }

    public Metric getMetric(String includedInKey) {
        for (String key : metrics.keySet()) {
            if (key.contains(includedInKey)) {
                return metrics.get(key);
            }
        }
        return null;
    }
}

