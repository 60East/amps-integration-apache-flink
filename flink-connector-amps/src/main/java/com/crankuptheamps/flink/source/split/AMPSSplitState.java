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

import java.nio.charset.StandardCharsets;

import com.crankuptheamps.client.fields.Field;

import org.apache.flink.annotation.Internal;

/**
 * Represents a mutable split of the {@link com.crankuptheamps.flink.source.AMPSSource}. 
 *
 * In the case of AMPS, a split is
 * a subscription to a topic along with an optional filter/bookmark
 * for parallelism/checkpointing purposes.
 */
@Internal
public class AMPSSplitState {

    /** The AMPS topic to subscribe to. */
    private final String topic;
    /** The content filter that will act as a split for readers. */
    private final String splitFilter;
    /** The original bookmark when this split was initialized. */
    private final String originalBookmark;
    /** The subscription ID of the split used to access the most recent bookmark. */
    private Field subId;
    /** The bytes of the split's topic. */
    private transient byte[] topicBytes = null;
    /** The bookmarks discarded since the last checkpoint. */
    private long bookmarksDiscarded = 0;

    /**
     * Creates a new split.
     *
     * @param topic The AMPS topic to subscribe to.
     * @param splitFilter The content filter that a client will use.
     */
    public AMPSSplitState(String topic, String splitFilter) {
        this.topic = topic;
        this.splitFilter = splitFilter;
        this.originalBookmark = "";
    }

    /**
     * Creates a new split with a subId.
     *
     * @param topic The AMPS topic to subscribe to.
     * @param splitFilter The content filter that a client will use.
     * @param subId The subId of this split.
     */
    public AMPSSplitState(String topic, String splitFilter, Field subId) {
        this.topic = topic;
        this.splitFilter = splitFilter;
        this.originalBookmark = "";
        this.subId = subId;
    }
    /**
     * Creates a new split with a bookmark.
     *
     * @param topic The AMPS topic to subscribe to.
     * @param splitFilter The content filter that a client will use.
     * @param bookmark The bookmark that will be used if the subscription id is not set.
     */
    public AMPSSplitState(String topic, String splitFilter, String bookmark) {
        this.topic = topic;
        this.splitFilter = splitFilter;
        this.originalBookmark = bookmark;
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
     * Returns the subId of this split.
     *
     * @return The subId.
     */
    public Field getSubId() {
        return subId;
    }

    /**
     * Changes the subId of this split.
     */
    public void setSubId(Field subId) {
        this.subId = subId;
    }

    /**
     * Returns the topic in bytes.
     *
     * @return The byte array for the topic.
     */
    public byte[] getTopicBytes() {
        if (topicBytes == null) topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        return topicBytes;
    }

    /**
     * Returns the original bookmark.
     *
     * @return The original bookmark of this.
     */
    public String getBookmark() {
        return originalBookmark;
    }

    /**
     * Returns the split as an {@link com.crankuptheamps.flink.source.split.AMPSSplit}.
     *
     * @return An immutable equivalent of this.
     */
    public AMPSSplit toAMPSSplit(String bookmark) {
        if (bookmark == null || bookmark.isEmpty()) {
            return new AMPSSplit(topic, splitFilter, originalBookmark);
        } else {
            return new AMPSSplit(topic, splitFilter, bookmark);
        }
    }

    /**
     * Increments the amount of bookmarks discarded from this split.
     */
    public void incBookmarksDiscarded() {
        bookmarksDiscarded++;
    }

    /**
     * Returns the amount of bookmarks discarded from this split.
     *
     * @return The amount of bookmarks discarded.
     */
    public long getBookmarksDiscarded() {
        return bookmarksDiscarded;
    }

    /**
     * Resets the amount of bookmarks discarded.
     *
     * Should be called on checkpoint completion.
     */
    public void resetBookmarksDiscarded() {
        this.bookmarksDiscarded = 0;
    }
}

