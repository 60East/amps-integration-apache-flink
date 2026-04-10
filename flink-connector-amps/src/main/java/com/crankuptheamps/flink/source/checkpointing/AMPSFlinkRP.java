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

import java.nio.charset.StandardCharsets;

import com.crankuptheamps.client.RecoveryPoint;
import com.crankuptheamps.client.fields.BookmarkField;
import com.crankuptheamps.client.fields.Field;

import org.apache.flink.annotation.Internal;

/**
 * {@link RecoveryPoint} that is constructed when resuming a bookmark sub with a Flink checkpoint.
 */
@Internal
public class AMPSFlinkRP implements RecoveryPoint {
    private final Field subId;
    private final BookmarkField bookmark;

    public AMPSFlinkRP(Field subId, BookmarkField bookmark) {
        this.subId = subId;
        this.bookmark = bookmark;
    }

    public AMPSFlinkRP(Field subId, String bookmark) {
        this.subId = subId;
        
        this.bookmark = new BookmarkField();
        this.bookmark.setValue(bookmark, StandardCharsets.UTF_8.newEncoder());
    }

    public AMPSFlinkRP(String subId, String bookmark) {
        this.subId = new Field(subId);
        
        this.bookmark = new BookmarkField();
        this.bookmark.setValue(bookmark, StandardCharsets.UTF_8.newEncoder());
    }

    @Override
    public RecoveryPoint copy() {
        return new AMPSFlinkRP(subId.copy(), bookmark.copy());
    }

    @Override
    public BookmarkField getBookmark() {
        return bookmark;
    }

    @Override
    public Field getSubId() {
        return subId;
    }
}

