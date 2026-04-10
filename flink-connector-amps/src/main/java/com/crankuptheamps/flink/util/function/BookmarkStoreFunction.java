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

package com.crankuptheamps.flink.util.function;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.util.SerializableFunction;

import org.apache.flink.annotation.Public;

/**
 * Functional interface used to supply a {@link BookmarkStore}.
 * The parameter passed to {@link SerializableFunction#apply(T) apply} 
 * will always be String of the client name that the {@link com.crankuptheamps.flink.source.reader.AMPSSourceReader}
 * uses for its {@link com.crankuptheamps.client.HAClient}. The client name
 * is unique for an individual job, but may be duplicated if multiple jobs
 * use the same client name String in the {@link com.crankuptheamps.flink.source.AMPSSource}.
 *
 * <p>
 * A simple {@link com.crankuptheamps.flink.source.AMPSSource} that uses this in its builder is
 * listed below.
 *
 * <pre>
 * <code>
 *AMPSSource<String> source = AMPSSource.<String>builder()
 *    .setUri("tcp://ampsserver.example.com:9007/json")
 *    .setTopic("json-topic")
 *    .setDeserializationSchema(new SimpleStringSchema())
 *    .setBookmark("0")
 *    .setBookmarkStoreFunction(
 *        (clientName) -> {
 *            try {
 *                return new MemoryBookmarkStore();
 *            } catch (Exception e) {
 *                throw new RuntimeException(e);
 *            }
 *        }
 *    )
 *    .build();
 * </code>
 * </pre>
 */
@Public
@FunctionalInterface
public interface BookmarkStoreFunction extends SerializableFunction<String, BookmarkStore> {
    public static final long serialVersionUID = 1L;
}

