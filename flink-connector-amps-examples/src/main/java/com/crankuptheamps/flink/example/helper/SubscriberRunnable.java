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

import java.io.Console;
import java.io.Reader;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.MessageStream;

public class SubscriberRunnable implements Runnable {
    private final String uri;
    private final String topic;
    private final String bookmark;
    private final String filter;
    private final String options;
    private final String command;
    
    private MessageHandler mh = null;

    public SubscriberRunnable(String uri, String topic, String bookmark, String filter, String options, String command) {
        this.uri = uri;
        this.topic = topic;
        this.bookmark = bookmark;
        this.filter = filter;
        this.options = options;
        this.command = command;
    }

    public SubscriberRunnable(String uri, String topic, String bookmark, String filter, String options, String command, MessageHandler mh) {
        this.uri = uri;
        this.topic = topic;
        this.bookmark = bookmark;
        this.filter = filter;
        this.options = options;
        this.command = command;
        this.mh = mh;
    }

    @Override
    public void run() {
        try (Client client = new Client("runnable-sub-" + Math.random());) {
            client.connect(uri);
            client.logon();

            Command cmd = new Command(command)
                .setTopic(topic)
                .setBookmark(bookmark)
                .setFilter(filter)
                .setOptions(options);

            if (mh == null) {
                try (MessageStream ms = client.execute(cmd)) {
                    for (Message m : ms) {
                        if (m != null) {
                          System.out.println(m.getData());
                          if (!m.isCorrelationIdNull()) {
                              System.out.println("CorrelationId: " + m.getCorrelationId());
                          }
                        }
                    }
                }
            } else {
                CommandId subId = client.executeAsync(cmd, mh);

                Console c = System.console();
                Reader r = c.reader();
                while(r.read() == -1) {
                    Thread.sleep(1);
                }

                client.unsubscribe(subId);
            }
        } catch (InterruptedException ie) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

