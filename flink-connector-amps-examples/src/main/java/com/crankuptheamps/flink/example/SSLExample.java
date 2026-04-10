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

package com.crankuptheamps.flink.example;

import java.io.InputStream;
import java.security.KeyStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.TCPSTransport;
import com.crankuptheamps.flink.example.helper.Constants;
import com.crankuptheamps.flink.example.helper.PublisherRunnable;
import com.crankuptheamps.flink.sink.AMPSSink;
import com.crankuptheamps.flink.source.AMPSSource;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SSLExample {
    public static final String URI = Constants.URI.JSON;
    public static final String S_URI = Constants.URI.S_JSON;
    public static final String INITIAL_TOPIC = "initTopic";
    public static final String MODIFIED_TOPIC = "modTopic";

    public static void main(String[] args) throws Exception {
        Thread pubThread = new Thread(new PublisherRunnable(
            "pub-runnable",
            INITIAL_TOPIC,
            URI,
            1000000,
            1_000_000_000L, // around one message per second
            0,
            1,
            "example data"
        ));

        Client sub = new Client("sub-client");

        try {
            sub.connect(URI);
            sub.logon();

            // Subscribe to see the original messages published to AMPS
            sub.subscribe(new MessageHandler() {
                @Override
                public void invoke(Message message) {
                    System.out.println("Original: " + message.getData());
                }
            }, INITIAL_TOPIC, 0);

            // Subscribe to see the messages that Flink modifies and publishes to AMPS
            sub.subscribe(new MessageHandler() {
                @Override
                public void invoke(Message message) {
                    System.out.println("Modified: " + message.getData());
                }
            }, MODIFIED_TOPIC, 0);

            // Client that publishes the original messages to AMPS
            pubThread.start();

            doJob();
        } catch (Exception e) {
            Constants.checkForJobCancellationException(e);
        } finally {
            sub.close();
            pubThread.interrupt();
        }
    }

    // The job that reads from AMPS and publishes alerts
    private static void doJob() throws Exception {
        AMPSSource<String> source = AMPSSource.<String>builder()
            .setClientName("AMPSSource")
            .setUri(S_URI)
            .setTopic(INITIAL_TOPIC)
            .setDeserializationSchema(new SimpleStringSchema())
            .setConnectorInitializer(new SSLConnectorInitializer())
            .build();

        AMPSSink<String> sink = AMPSSink.<String>builder()
            .setClientName("AMPSSink")
            .setUri(S_URI)
            .setTopic(MODIFIED_TOPIC)
            .setSerializationSchema(new SimpleStringSchema())
            .setConnectorInitializer(new SSLConnectorInitializer())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "AMPS Source")
            .process(new ProcessFunction<String, String>() {
                // The part of the string used to locate where the field to change is
                private final String searchStr = "data\":\"";

                @Override
                public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                    int dataPart = value.indexOf(searchStr);
                    // This is what the value of "data" will be replaced with
                    String modifiedData = "" + Math.random();
                    // Output the string with the modified data
                    out.collect(value.substring(0, dataPart + searchStr.length()) + modifiedData + "\"}");
                }
            })
            .sinkTo(sink);

        env.execute("SSL AMPS Connector Example");
    }

    public static class SSLConnectorInitializer implements ConnectorInitializer {
        public String password = "password";

        public SSLConnectorInitializer() {}

        public SSLConnectorInitializer(String password) {
            this.password = password;
        }

        @Override
        public void init(HAClient client) throws Exception {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            KeyStore ts = KeyStore.getInstance("JKS");
            
            ClassLoader cl = SSLConnectorInitializer.class.getClassLoader();

            try (InputStream is = cl.getResourceAsStream("ssl/keystore.p12");) {
                ks.load(is, password.toCharArray());
            }
            
            try (InputStream is = cl.getResourceAsStream("ssl/truststore.jks");) {
                ts.load(is, password.toCharArray());
            }
            
            // Get the key manager factory, using the default
            // algorithm.
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            // Initialize the factory with the keystore.
            kmf.init(ks, password.toCharArray());
            tmf.init(ts);
            
            // Get the SSL context
            SSLContext context = SSLContext.getInstance("TLS");
            
            // Use the key manager just constructed, with defaults
            // for the trust manager and randomness source.
            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            
            // Set the SSLContext for the TCPS transport
            // to the context just set up with the keystore.
            TCPSTransport.setDefaultSSLContext(context);

            // Use a future to allow a timeout if the client takes too long to connect to AMPS.
            // This demonstrates that the ConnectorInitializer can connect the client to AMPS.
            // This can also be done by setting a reconnect delay strategy.
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                try {
                    client.connectAndLogon();
                    return true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).orTimeout(5, TimeUnit.SECONDS);

            future.join();
        }
    }
}

