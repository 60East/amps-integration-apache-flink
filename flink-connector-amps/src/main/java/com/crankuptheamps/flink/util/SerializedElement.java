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

package com.crankuptheamps.flink.util;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Wrapper to hold serialized data as well as some information that will be used
 * when publishing to AMPS.
 */
@PublicEvolving
public class SerializedElement {
    
    /** The data that will be published to AMPS. */
    private byte[] data;
    /** The correlation ID, if any, that will be used when publishing to AMPS. */
    private String correlationId;
    /** The SOW Key, if any, that will be used when publishing to AMPS. */
    private String sowKey;
    /** The topic, if any, that will be used when publishing to AMPS. If null, use topic set by setTopic in the AMPSSink builder. */
    private String topic;

    public SerializedElement() {}

    public SerializedElement(byte[] data) {
        this.data = data;
    }

    /**
     * Returns the data to publish to AMPS.
     *
     * Used internally to put the serialized data in a publish command.
     *
     * @return The data to publish to AMPS.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Sets the data to publish to AMPS.
     *
     * @param data The data to publish to AMPS.
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Returns if data is null.
     *
     * Used internally to determine if data will be sent.
     *
     * @return If data is null.
     */
    public boolean isDataNull() {
        return data == null;
    }

    /**
     * Returns the correlation ID to use for the SerializedElement.
     *
     * Used internally to set the correlation ID in a publish command.
     *
     * @return The correlation ID.
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID to use for the SerializedElement.
     *
     * @param correlationId The correlation ID.
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Returns if the correlation ID is null.
     *
     * Used internally to determine if the correlation ID for a publish command should be set.
     *
     * @return If correlation ID is null.
     */
    public boolean isCorrelationIdNull() {
        return correlationId == null;
    }
    
    /**
     * Returns the SOW Key to use for the SerializedElement.
     *
     * Used internally to set the SOW Key in a publish/sow_delete command.
     *
     * @return The SOW Key.
     */
    public String getSowKey() {
        return sowKey;
    }

    /**
     * Sets the SOW Key to use for the SerializedElement.
     *
     * @param sowKey The SOW Key.
     */
    public void setSowKey(String sowKey) {
        this.sowKey = sowKey;
    }

    /**
     * Returns if the SOW Key is null.
     *
     * Used internally to determine if the SOW Key for a publish/sow_delete command should be set.
     *
     * @return If SOW Key is null.
     */
    public boolean isSowKeyNull() {
        return sowKey == null;
    }
    
    /**
     * Returns the topic used for publishing the SerializedElement's data.
     *
     * Used internally to set the topic.
     *
     * @return The topic.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets the topic used for publishing the SerializedElement's data.
     *
     * @param topic The topic.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Returns if the topic is null.
     *
     * Used internally to determine if the topic should be set using the SerializedElement's topic.
     *
     * @return If the topic is null.
     */
    public boolean isTopicNull() {
        return topic == null;
    }
}

