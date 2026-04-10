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
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.crankuptheamps.client.RecoveryPoint;
import com.crankuptheamps.client.RecoveryPointAdapter;
import com.crankuptheamps.client.fields.Field;

import org.apache.flink.annotation.Internal;

/**
 * Class to help an {@link com.crankuptheamps.flink.source.reader.AMPSSourceReader} continue a subscription from a Flink checkpoint.
 *
 * This class is only used when starting subscriptions, so update is a no-op.
 */
@Internal
public class AMPSFlinkRPA implements RecoveryPointAdapter {

    private final List<RecoveryPoint> points = new ArrayList<>();
    
    private int index = -1;

    /**
     * Used to initialize the adapter with the bookmarks from the last saved Flink checkpoint.
     */
    public void init(RecoveryPoint recoveryPoint) {
        points.add(recoveryPoint);
    }

    // RecoveryPointAdapter

    /**
     * The adapter is only used when initializing a new bookmark store, so this is a no-op.
     */
    @Override
    public void update(RecoveryPoint recoveryPoint) throws Exception {}

    @Override
    public void purge(Field subId) throws Exception {}

    @Override
    public void purge() throws Exception {}

    // Iterable
    
    @Override
    public Iterator<RecoveryPoint> iterator() {
        return points.iterator();
    }

    @Override
    public void forEach(Consumer<? super RecoveryPoint> action) {
        points.forEach(action);
    }

    @Override
    public Spliterator<RecoveryPoint> spliterator() {
        return points.spliterator();
    }

    // Iterator
    
    @Override
    public boolean hasNext() {
        return index + 1 < points.size();
    }

    @Override
    public RecoveryPoint next() {
        index++;
        return points.get(index);
    }

    // Autocloseable

    @Override
    public void close() throws Exception {}
}

