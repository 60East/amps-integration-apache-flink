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

import java.util.concurrent.LinkedBlockingQueue;

import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;

public class AMPSMessageHandler implements MessageHandler {
    public final LinkedBlockingQueue<String> queue;
    public final LinkedBlockingQueue<String> correlationIds;
    public final LinkedBlockingQueue<String> sowKeys;
    public final LinkedBlockingQueue<String> timestamps;

    public AMPSMessageHandler() {
        queue = new LinkedBlockingQueue<>(5);
        correlationIds = new LinkedBlockingQueue<>(5);
        sowKeys = new LinkedBlockingQueue<>(5);
        timestamps = new LinkedBlockingQueue<>(5);
    }

    public AMPSMessageHandler(int size) {
        queue = new LinkedBlockingQueue<>(size);
        correlationIds = new LinkedBlockingQueue<>(size);
        sowKeys = new LinkedBlockingQueue<>(size);
        timestamps = new LinkedBlockingQueue<>(size);
    }

    @Override
    public void invoke(Message message) {
        try {
            queue.put(message.getData());
            if (!message.isCorrelationIdNull()) {
                correlationIds.put(message.getCorrelationId());
            }
            if (!message.isSowKeyNull()) {
                sowKeys.put(message.getSowKey());
            }
            if (!message.isTimestampNull()) {
                timestamps.put(message.getTimestamp());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
