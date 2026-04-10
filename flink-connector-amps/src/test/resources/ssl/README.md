To recreate the certificate and key for the tests that use SSL, run the following:

1. Make cert.pem and key.pem for the AMPS instance (testConfig.xml)
    ```
    openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 99999 -out cert.pem
    ```

2. Make key store (use the password "password")
    ```
    openssl pkcs12 -export -in cert.pem -inkey key.pem -out keystore.p12 -name testKeyStore
    ```

3. Make trust store (use the password "password")
    ```
    keytool -importcert -alias testCert -file cert.pem -keystore truststore.jks
    ```

