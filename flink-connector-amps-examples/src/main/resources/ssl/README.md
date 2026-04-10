To recreate the certificate and key for the example that uses SSL, run the following:

1. Make cert.pem and key.pem for the AMPS instance (config.xml)
    ```
    openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 99999 -out cert.pem
    ```

2. Make key store (use the password "password")
    ```
    openssl pkcs12 -export -in cert.pem -inkey key.pem -out keystore.p12 -name exampleKeyStore
    ```

3. Make trust store (use the password "password")
    ```
    keytool -importcert -alias exampleCert -file cert.pem -keystore truststore.jks
    ```

