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

public class AMPSPojo {
    public long id;
    public String data;
    public long num;

    public AMPSPojo() {
        id = 0;
        data = "";
        num = 0;
    }

    public AMPSPojo(long id, String data) {
        this.id = id;
        this.data = data;
        this.num = id;
    }

    public AMPSPojo(long id, String data, long num) {
        this.id = id;
        this.data = data;
        this.num = num;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof AMPSPojo)) return false;
        AMPSPojo tObj = (AMPSPojo) obj;
        return tObj.id == id &&
            tObj.data.equals(data) &&
            tObj.num == num;
    }

    @Override
    public String toString() {
        return "{\"id\":" + id + ",\"data\":\"" + data + "\",\"num\":" + num + "}";
    }
}
