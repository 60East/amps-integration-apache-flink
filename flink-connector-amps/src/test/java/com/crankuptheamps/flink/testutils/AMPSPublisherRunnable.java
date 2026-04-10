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

import com.crankuptheamps.client.Client;

public class AMPSPublisherRunnable implements Runnable {
    public String uri;
    public String topic;
    public String data = "";
    public long sleepDuration = 250;
    public int max = 5;
    public static final String MESSAGE_FORMAT = "{\"id\":%d,\"data\":\"%s\"}";

    public AMPSPublisherRunnable() {}

    public AMPSPublisherRunnable(String uri, String topic, String data, long sleepDuration, int max) {
        this.uri = uri;
        this.topic = topic;
        this.data = data;
        this.sleepDuration = sleepDuration;
        this.max = max;
    }

    @Override
    public void run() {
        try (Client client = new Client(topic + "-pub");) {
            client.connect(uri);
            client.logon();

            for (int i = 0; i < max; i++) {
                Thread.sleep(sleepDuration);
                client.publish(topic, String.format(MESSAGE_FORMAT, i, data));
            }
        } catch (InterruptedException ie) {
            // Ignore
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
