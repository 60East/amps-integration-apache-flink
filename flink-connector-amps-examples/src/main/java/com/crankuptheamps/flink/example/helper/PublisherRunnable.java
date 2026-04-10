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

import java.util.concurrent.TimeUnit;

import com.crankuptheamps.client.Client;

public class PublisherRunnable implements Runnable {
    private final String clientName;
    private final String topic;
    private final String uri;
    private final String msg;
    private final long sleepInNano;
    private final long start;
    private final long inc;
    private final long max;

    public PublisherRunnable(String clientName, String topic, String uri, long max, long sleepInNano, long start, long inc) {
        this.clientName = clientName;
        this.topic = topic;
        this.uri = uri;
        this.max = max;
        this.sleepInNano = sleepInNano;
        this.start = start;
        this.inc = inc;
        this.msg = "a".repeat(480);
    }
    
    public PublisherRunnable(String clientName, String topic, String uri, long max, long sleepInNano, long start, long inc, String msg) {
        this.clientName = clientName;
        this.topic = topic;
        this.uri = uri;
        this.max = max;
        this.sleepInNano = sleepInNano;
        this.start = start;
        this.inc = inc;
        this.msg = msg;
    }

    @Override
    public void run() {
        if (max < 1) return;

        try (Client client = new Client(clientName);) {
            // Wait a short duration for Flink job to finish submitting 
            Thread.sleep(3000);

            client.connect(uri);
            client.logon();

            if (sleepInNano == -1) {
                for (long i = start; i <= max; i += inc) {
                    client.publish(topic, "{\"num\":" + i + ",\"data\":\"" + msg + "\"}");
                }
            } else {
                for (long i = start; i <= max; i += inc) {
                    client.publish(topic, "{\"num\":" + i + ",\"data\":\"" + msg + "\"}");
                    TimeUnit.NANOSECONDS.sleep(sleepInNano);
                }
            }
        } catch (InterruptedException ie) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

