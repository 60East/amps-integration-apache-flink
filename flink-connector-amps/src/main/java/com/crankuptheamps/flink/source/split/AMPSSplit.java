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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;

/**
 * Represents a {@link SourceSplit} of the {@link com.crankuptheamps.flink.source.AMPSSource}. 
 *
 * In the case of AMPS, a split is
 * a subscription to a topic along with an optional filter/bookmark
 * for parallelism/checkpointing purposes.
 */
@PublicEvolving
public class AMPSSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /** The AMPS topic to subscribe to. */
    private final String topic;
    /** The content filter that will act as a split for readers. */
    private final String splitFilter;
    /** The most recent bookmark that should be used to resume a subscription on failure. */
    private final String bookmark;
    /** The bytes of this split's topic. */
    private transient byte[] topicBytes = null;
    /** The split ID of this split. */
    private transient String splitId = null;

    /**
     * Creates a new split.
     *
     * @param topic The AMPS topic to subscribe to.
     */
    public AMPSSplit(String topic) {
        this.topic = topic;
        this.splitFilter = "";
        this.bookmark = "";
        
        topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        setUpSplitId();
    }

    /**
     * Creates a new split with a content filter.
     *
     * @param topic The AMPS topic to subscribe to.
     * @param splitFilter The content filter that a client will use.
     */
    public AMPSSplit(String topic, String splitFilter) {
        this.topic = topic;
        this.splitFilter = splitFilter;
        this.bookmark = "";
        
        topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        setUpSplitId();
    }

    /**
     * Creates a new split with a content filter and bookmark.
     *
     * @param topic The AMPS topic to subscribe to.
     * @param splitFilter The content filter that a client will use.
     * @param bookmark The bookmark that this split should resume from.
     */
    public AMPSSplit(String topic, String splitFilter, String bookmark) {
        this.topic = topic;
        this.splitFilter = splitFilter;
        this.bookmark = bookmark;
        
        topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        setUpSplitId();
    }

    /**
     * Returns the AMPS topic of this split.
     *
     * @return The AMPS topic.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Returns the AMPS content filter of this split.
     * 
     * @return The AMPS content filter.
     */
    public String getSplitFilter() {
        return splitFilter;
    }

    /**
     * Returns the bookmark of this split.
     *
     * @return The bookmark.
     */
    public String getBookmark() {
        return bookmark;
    }

    /**
     * Returns the topic in bytes.
     *
     * @return The byte array for the topic.
     */
    public byte[] getTopicBytes() {
        return topicBytes;
    }

    /**
     * Returns the split as an {@link com.crankuptheamps.flink.source.split.AMPSSplitState}.
     *  
     * @return The mutable equivalent of this.
     */
    public AMPSSplitState toAMPSSplitState() {
        return new AMPSSplitState(topic, splitFilter, bookmark);
    }

    /**
     * Returns the ID of this split.
     *
     * @return The split ID.
     */
    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return String.format("AMPSSplit{topic='%s',filter='%s',bookmark='%s'}", topic, splitFilter, bookmark);
    }

    private void setUpSplitId() {
        splitId = topic + "~" + splitFilter;
    }
}

