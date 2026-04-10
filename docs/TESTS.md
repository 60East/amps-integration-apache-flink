# AMPS Flink Connector Tests

The AMPS Flink connectors include several junit5 tests to check that intended functionality is maintained when modifications are made.
For the connectors in `flink-connector-amps/`, the tests are located in the directory `flink-connector-amps/src/test/java/com/crankuptheamps/flink/` along with several utility classes in the `flink-connector-amps/src/test/java/com/crankuptheamps/flink/testutils/` directory.
Resources for tests related to SSL are located in the directory `flink-connector-amps/src/test/resources/ssl` along with a `README.md` on how to create the resources.
If the directory only has the `README.md`, you will need to generate the SSL resources.

## Generate SSL Resources

Several tests include SSL, so in order to run all the tests, the SSL resources must be generated.
To create the SSL resources necessary for the tests, do the following:

1. **Navigate to the test resources directory**

    ```
    cd path/to/flink-connector-amps/src/test/resources/ssl
    ```

2. **Create the resources using the** [**README**](../flink-connector-amps/src/test/resources/ssl/README.md)

    - The instructions below are the same as those found in the above README

3. **Make `cert.pem` and `key.pem`**

    ```
    openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 99999 -out cert.pem
    ```

4. **Make key store (use the password "password")**

    ```
    openssl pkcs12 -export -in cert.pem -inkey key.pem -out keystore.p12 -name testKeyStore
    ```

5. **Make trust store (use the password "password")**

    ```
    keytool -importcert -alias testCert -file cert.pem -keystore truststore.jks
    ```

## Running the Tests

1. **Navigate to the parent directory**

    ```
    cd path/to/amps-integration-apache-flink
    ```

2. **Ensure at least Maven 3.6.3 is installed**

    ```
    mvn -v
    ```

3. **Ensure an AMPS instance of at least version 5.3.5 is running using the `amps-configs/testConfig.xml` configuration**

    - This AMPS instance provides the topics needed for the tests

        ```
        ~/AMPS-{version}-Release-Linux/bin/ampServer amps-configs/testConfig.xml
        ```

4. **Run the tests**

    ```
    mvn clean test -DskipTests=false
    ```

Test results will be printed in the console. They can also be found in the directory `flink-connector-amps/target/surefire-reports/`.

## Test Coverage Report

A test coverage report is accessible after running the tests.

The following can be done to open the report:

1. **Navigate to the parent directory**

    ```
    cd path/to/amps-integration-apache-flink
    ```

2. **Navigate to the directory with the test coverage report**

    ```
    cd flink-connector-amps/target/site/jacoco
    ```

3. **Open the report**
    
    - Open `index.html` to view the test coverage report

## Test Logs

Logs from the connector are accessible after running the tests.
The logs are located in `flink-connector-amps/target/logs-surefire.txt`.
By default, the logging level used is DEBUG. This can be changed by modifying the `<testLoggingLevel>` property in the `pom.xml`, or by using the `-DtestLoggingLevel={level}` argument when running the tests.

## AMPSSource Tests

Unit tests related to the classes that focus on the AMPSSource.

The following files contain unit tests for classes related to the AMPSSource:
- **AMPSFlinkRPATest.java**
    - Tests related to the internal **RecoveryPointAdapter** that is used during checkpointing
- **AMPSSourceReaderTest.java**
    - Tests related to the **SourceReader** that consumes data from AMPS
    - Requires an AMPS instance running with the config `amps-configs/testConfig.xml`
- **AMPSSplitEnumeratorTest.java**
    - Tests related to the **SplitEnumerator** that assigns splits to a **SourceReader**
- **AMPSCheckpointSerializerTest.java**
    - Tests related to the serialization/deserialization of an **AMPSCheckpoint**
- **AMPSRecordEmitterTest.java**
    - Tests related to the **RecordEmitter** that emits data from the **SourceReader** to Flink
- **AMPSSplitSerializerTest.java**
    - Tests related to the serialization/deserialization of an **AMPSSplit**
- **AMPSSplitReaderTest.java**
    - Tests related to the **SplitReader** component of the **AMPSSourceReader**
    - Requires an AMPS instance running with the config `amps-configs/testConfig.xml`

## AMPSSink Tests

Unit tests related to the classes that focus on the AMPSSink.

The following files contain unit tests for classes related to the AMPSSink:
- **AMPSStatefulSinkWriterTest.java**
    - Tests related to the **SinkWriter** that publishes data to AMPS
    - Requires an AMPS instance running with the config `amps-configs/testConfig.xml`
- **AMPSWriterStateSerializerTest.java**
    - Tests related to the serialization/deserialization of an **AMPSWriterState**

## Flink Job Tests

Unit tests that simulate simple Flink jobs that can involve both AMPSSources and AMPSSinks. These utilize a Flink **MiniClusterExtension** to allow the tests to run without submitting jobs to a Flink cluster.

The following files contain unit tests for classes that involve Flink jobs:
- **AMPSFlinkJobTest.java**
    - Tests that utilize a Flink **MiniClusterExtension** to submit and execute Flink jobs
    - Requires an AMPS instance running with the config `amps-configs/testConfig.xml`

