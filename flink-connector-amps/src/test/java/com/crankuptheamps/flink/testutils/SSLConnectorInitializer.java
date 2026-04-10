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

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.TCPSTransport;
import com.crankuptheamps.flink.util.function.ConnectorInitializer;

public class SSLConnectorInitializer implements ConnectorInitializer {
    public String password = "password";

    public SSLConnectorInitializer() {}

    public SSLConnectorInitializer(String password) {
        this.password = password;
    }

    @Override
    public void init(HAClient client) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        KeyStore ts = KeyStore.getInstance("JKS");

        String pathToKeyStore = SSLConnectorInitializer.class.getClassLoader().getResource("ssl/keystore.p12").getPath();
        String pathToTrustStore = SSLConnectorInitializer.class.getClassLoader().getResource("ssl/truststore.jks").getPath();

        try (FileInputStream fis = new FileInputStream(pathToKeyStore);) {
            ks.load(fis, password.toCharArray());
        }
        
        try (FileInputStream fis = new FileInputStream(pathToTrustStore);) {
            ts.load(fis, password.toCharArray());
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
    }
}

