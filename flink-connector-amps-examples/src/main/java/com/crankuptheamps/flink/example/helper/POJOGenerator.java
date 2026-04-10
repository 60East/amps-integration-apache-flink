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

package com.crankuptheamps.flink.example.helper;

import java.util.UUID;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class POJOGenerator extends DataGeneratorSource<OuterPOJO> {
    private static int incNum = 1;

    public POJOGenerator(long count, RateLimiterStrategy<?> rls) {
        super(new POJOGeneratorFunction(), count, rls, TypeInformation.of(OuterPOJO.class));
    }

    public static final String[] USERNAMES = new String[]{"abb", "al", "joe", "steve", "charles", "bob", "peter"};
    public static final String[] ITEMS = new String[]{"A", "AA", "CSB", "LSL", "GQ", "WO", "ZES", "CX", "OVS", "EN"};
    private static final String DATA = "a".repeat(390);

    public static InnerPOJO makeInner() {
        InnerPOJO res = new InnerPOJO();

        res.setStr(DATA);
        res.setUsername(USERNAMES[(int) Math.floor(Math.random() * USERNAMES.length)]);
        res.setNum((int) Math.floor(Math.random() * 100000));

        return res;
    }

    public static OuterPOJO makeOuter() {
        OuterPOJO res = new OuterPOJO();

        res.setId(UUID.randomUUID().toString());

        int i = (int) Math.floor(Math.random() * ITEMS.length);

        res.setItemId(incNum);
        incNum++;

        res.setName(ITEMS[i]);

        res.setBuyer(makeInner());

        return res;
    }
}

class POJOGeneratorFunction implements GeneratorFunction<Long, OuterPOJO> {
    @Override
    public OuterPOJO map(Long value) throws Exception {
        return POJOGenerator.makeOuter();
    }
}
