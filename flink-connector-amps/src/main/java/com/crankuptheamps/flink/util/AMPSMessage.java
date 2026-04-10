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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.crankuptheamps.client.fields.CommandField;
import com.crankuptheamps.client.fields.Field;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Wrapper used for preserving only the necessary data from messages coming from AMPS. 
 */
@PublicEvolving
public class AMPSMessage {
    
    /** The message data from AMPS. */
    private byte[] data;
    /** AckType of the message. Used to determine when a subscription is finished. */
    private int ackType;
    /** The bookmark bytes of the message. Used for acking. */
    private byte[] bookmark;
    /** The bookmark sequence number of the message. Used for discarding bookmarks. */
    private long bookmarkSeqNo;
    /** The {@link com.crankuptheamps.client.Message.Command} of this message. Used for determining which messages to emit. */
    private int command;
    /** The AMPS timestamp of a message in bytes. Used to emit a timestamp to Flink. */
    private byte[] timestamp;
    /** The topic of the message in bytes. Used for acking. */
    private byte[] topic;
    /** Index of the split the message was read from. Used for accessing the split ID of an AMPSSplit. */
    private int splitIndex;
    /** The subscription ID of the message. Used for discarding bookmarks. */
    private Field subId;
    /** The charset for decoding and encoding byte arrays. */
    private Charset charset;
    /** The preserved header keys. */
    private Map<AMPSSourceHeaderKeys, String> headerMap;

    public AMPSMessage() {
        this.charset = StandardCharsets.UTF_8;
    }

    public AMPSMessage(byte[] data, Charset charset) {
        this.data = data;
        this.charset = charset;
    }

    public AMPSMessage(byte[] data) {
        this.data = data;
        this.charset = StandardCharsets.UTF_8;
    }

    /**
     * Returns the payload of the AMPSMessage.
     *
     * @return The raw payload.
     */
    public byte[] getDataRaw() {
        return data;
    }

    /**
     * Returns the payload of the AMPSMessage.
     *
     * @return The decoded payload as a String.
     */
    public String getData() {
        return data == null ? null : new String(data, charset);
    }

    /**
     * Sets the payload using a byte array.
     *
     * @param data The data payload.
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Sets the payload using a String.
     *
     * @param data The data payload as a string that will be converted to a byte array.
     */
    public void setData(String data) {
        this.data = data.getBytes(charset);
    }
 
    /**
     * Returns the {@link com.crankuptheamps.client.Message.AckType} for the AMPSMessage.
     *
     * Used internally to determine when a command is finished and a reader should close.
     *
     * @return The ack type integer.
     */
    public int getAckType() {
        return ackType;
    }

    /**
     * Sets the {@link com.crankuptheamps.client.Message.AckType} for the AMPSMessage.
     *
     * @param ackType The ack type integer.
     */
    public void setAckType(int ackType) {
        this.ackType = ackType;
    }

    /**
     * Returns the raw value for the AMPS Bookmark.
     *
     * Used internally for acking messages.
     *
     * @return The raw value for the AMPS Bookmark.
     */
    public byte[] getBookmarkRaw() {
        return bookmark;
    }

    /**
     * Returns the AMPS Bookmark for the AMPSMessage.
     *
     * @return The AMPS Bookmark for the AMPSMessage.
     */
    public String getBookmark() {
        return bookmark == null ? null : new String(bookmark, charset);
    }

    /**
     * Sets the value for the AMPS Bookmark.
     *
     * @param bookmark The byte array of the bookmark.
     */
    public void setBookmark(byte[] bookmark) {
        this.bookmark = bookmark;
    }

    /**
     * Sets the value for the AMPS Bookmark.
     *
     * @param bookmark The String of the bookmark that will be converted to a byte array.
     */
    public void setBookmark(String bookmark) {
        this.bookmark = bookmark.getBytes(charset);
    }

    /**
     * Returns the bookmark sequence number for the AMPSMessage.
     *
     * Used internally for discarding bookmarks.
     *
     * @return The bookmark sequence number for the AMPSMessage.
     */
    public long getBookmarkSeqNo() {
        return bookmarkSeqNo;
    }

    /**
     * Sets the bookmark sequence number for the AMPSMessage.
     *
     * @param bookmarkSeqNo The bookmark sequence number.
     */
    public void setBookmarkSeqNo(long bookmarkSeqNo) {
        this.bookmarkSeqNo = bookmarkSeqNo;
    }

    /**
     * Returns the {@link com.crankuptheamps.client.Message.Command} for the AMPSMessage, indicating the type of message it is.
     *
     * Used internally in the {@link com.crankuptheamps.flink.source.reader.deserializer.AMPSDataOnlyDeserializationSchemaWrapper}
     * for determining which messages should be emitted.
     *
     * @return The Command for the AMPSMessage.
     */
    public int getCommand() {
        return command;
    }
    
    /**
     * Sets the {@link com.crankuptheamps.client.Message.Command} for the AMPSMessage, indicating the type of message it is.
     *
     * @param command The command for the AMPSMessage.
     */
    public void setCommand(int command) {
        this.command = command;
    }

    /**
     * Returns the raw value for the timestamp for the AMPSMessage, an ISO-8601 formatted String.
     *
     * Used internally to emit a timestamp to Flink.
     * 
     * @return The raw timestamp for the AMPSMessage.
     */
    public byte[] getTimestampRaw() {
        return timestamp;
    }

    /**
     * Returns the timestamp for the AMPSMessage, an ISO-8601 formatted String.
     *
     * @return The timestamp for the AMPSMessage as an ISO-8601 formatted String.
     */
    public String getTimestamp() {
        return timestamp == null ? null : new String(timestamp, charset);
    }

    /**
     * Sets the timestamp for the AMPSMessage.
     *
     * @param timestamp The raw timestamp.
     */
    public void setTimestamp(byte[] timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns the topic that the AMPSMessage applies to.
     *
     * Used internally for acking messages.
     *
     * @return The raw topic that the AMPSMessage applies to.
     */
    public byte[] getTopicRaw() {
        return topic;
    }

    /**
     * Returns the topic that the AMPSMessage applies to.
     *
     * @return The topic that the AMPSMessage applies to.
     */
    public String getTopic() {
        return topic == null ? null : new String(topic, charset);
    }
    
    /**
     * Sets the topic that the AMPSMessage applies to.
     *
     * @param topic The raw topic that the AMPSMessage applies to.
     */
    public void setTopic(byte[] topic) {
        this.topic = topic;
    }
    
    /**
     * Sets the topic that the AMPSMessage applies to.
     *
     * @param topic The topic that the AMPSMessage applies to.
     */
    public void setTopic(String topic) {
        this.topic = topic.getBytes(charset);
    }

    /**
     * Returns an integer used to keep track of the splits that any individual {@link com.crankuptheamps.flink.source.reader.AMPSSourceReader} has.
     *
     * A reader might have multiple splits, so this field helps separate AMPSMessages from differing splits.
     * Used internally for accessing the split ID of an {@link com.crankuptheamps.flink.source.split.AMPSSplit}.
     *
     * @return An integer that represents which split the AMPSMessage came from.
     */
    public int getSplitIndex() {
        return splitIndex;
    }

    /**
     * Sets an integer used to keep track of the splits that any individual {@link com.crankuptheamps.flink.source.reader.AMPSSourceReader} has.
     *
     * A reader might have multiple splits, so this field helps separate AMPSMessages from differing splits.
     *
     * @param splitIndex An integer that represents which split the AMPSMessage came from.
     */
    public void setSplitIndex(int splitIndex) {
        this.splitIndex = splitIndex;
    }
    
    /**
     * Sets the SubId of the AMPSMessage.
     *
     * @param subId The subId to be used for the AMPSMessage.
     */
    public void setSubId(Field subId) {
        this.subId = subId;
    }

    /**
     * Returns the SubId of the AMPSMessage.
     *
     * Used internally for discarding bookmarks.
     *
     * @return The raw SubId of the AMPSMessage.
     */
    public Field getSubIdRaw() {
        return subId;
    }

    /**
     * Returns the SubId of the AMPSMessage.
     *
     * @return The SubId of the AMPSMessage as a String.
     */
    public String getSubId() {
        return subId == null ? null : subId.toString();
    }

    /**
     * Puts a header into the map.
     *
     * Used to keep track of header values.
     *
     * @param headerKey The header key.
     * @param header The header value to store.
     */
    public void putHeader(AMPSSourceHeaderKeys headerKey, String header) {
        if (headerMap == null) {
            headerMap = new HashMap<>();
        }

        headerMap.put(headerKey, header);
    }

    /**
     * Returns a header from the map.
     *
     * Used to access the preserved headers.
     *
     * @param headerKey The header key.
     * @return The header value, or null if it does not exist.
     */
    public String getHeader(AMPSSourceHeaderKeys headerKey) {
        if (headerMap == null) {
            headerMap = new HashMap<>();
        }

        String header = headerMap.get(headerKey);

        if (header == null) {
            switch (headerKey) {
                case TOPIC -> header = getTopic();
                case BOOKMARK -> header = getBookmark();
                case TIMESTAMP -> header = getTimestamp();
                case SUBSCRIPTION_ID -> header = getSubId();
                case COMMAND -> header = CommandField.encodeCommand(command);
                case LENGTH -> header = "" + data.length;
                default -> {
                    return null;
                }
            }

            headerMap.put(headerKey, header);
        }

        return header;
    }

    @Override
    public String toString() {
        return "AMPSMessage{" + 
            "\"topic\":\"" + getTopic() + "\"," +
            "\"dataLength\":" + (data == null ? 0 : data.length) + "," +
            "\"bookmark\":\"" + getBookmark() + "\"," +
            "\"bookmarkSeqNo\":" + bookmarkSeqNo + "," +
            "\"timestamp\":\"" + getTimestamp() + "\"," +
            "\"subId\":\"" + getSubId() + "\"," +
            "\"ackType\":" + ackType + "," +
            "\"splitIndex\":" + splitIndex + "," +
            "\"headerCount\":" + (headerMap == null ? 0 : headerMap.size()) + "}";
    }
}

