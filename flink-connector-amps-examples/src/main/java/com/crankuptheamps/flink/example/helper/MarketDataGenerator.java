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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class MarketDataGenerator extends DataGeneratorSource<MarketData> {
    public MarketDataGenerator(long count, RateLimiterStrategy<?> rls) {
        super(new MarketDataGeneratorFunction(), count, rls, TypeInformation.of(MarketData.class));
    }

    public static final String[] ITEMS = new String[]{
        "MMM", "ABBV", "ALV", "GOOGL", "AMZN", "AMGN", "ABI", "APPL", "BHP", "BA", "BP",
        "BATS", "CVX", "CSCO", "C", "KO", "DD", "XOM", "FB", "GE", "GSK", "HSBA", "INTC",
        "IBM", "JNJ", "JPM", "MA", "MCD", "MRK", "MSFT", "NESN", "NOVN", "NVDA", "ORCL",
        "PEP", "PFE", "PM", "PG", "ROG", "RY", "RDSA", "SMSN", "SAN", "SIE", "TSM", "TOT",
        "V", "WMT", "DIS"
    };

    public static final String DATA = "a".repeat(450);

    public static MarketData makeMarketData() {
        MarketData res = new MarketData();

        res.symbol = ITEMS[(int) Math.floor(Math.random() * ITEMS.length)];
        res.ask = Math.random() * 1000;
        res.bid = Math.random() * 1000;
        res.data = DATA;

        return res;
    }
}

class MarketDataGeneratorFunction implements GeneratorFunction<Long, MarketData> {
    @Override
    public MarketData map(Long value) throws Exception {
        return MarketDataGenerator.makeMarketData();
    }
}
