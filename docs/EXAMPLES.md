# AMPS Flink Connector Examples

Several examples of the AMPSSource and AMPSSink being used in Flink jobs are included in the `flink-connector-amps-examples/src/main/java/com/crankuptheamps/flink/example/` directory.
These examples range from volume examples where large amounts of messages are consumed by an AMPSSource and then published back to AMPS by an AMPSSink to a small job that demonstrates how an AMPSSource works alongside Flink's checkpointing by printing messages to the console.
These examples' configuration can be modified by simply changing the constants at the top of any example's .java source file before rebuilding.
Resources for running an example where the connectors use SSL are located in the `flink-connector-amps-examples/src/main/resources/ssl/` directory.
These resources are required for running the SSL example.
If the directory only has the `README.md`, you will need to generate the SSL resources.

The jobs themselves can be changed for more fine-tuned experimentation.

## Prerequisites

- **Java 17 to 21**
    - [Java 17 is the recommended Java version to run Flink on](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/#java-17)
- **Maven 3.6.3 or higher**
- **AMPS Server**
- [**Apache Flink 2.2.0 or higher**](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/try-flink/local_installation/)

## Generate SSL Resources

In order to run `SSLExample.java`, the SSL resources must be generated.
If you do not wish to run this example, you may skip these steps.
To create the SSL resources necessary for running the example, do the following:

1. **Navigate to the examples resources directory**
    
    ```
    cd path/to/flink-connector-amps-examples/src/main/resources/ssl
    ```

2. **Create the resources using the** [**README**](../flink-connector-amps-examples/src/main/resources/ssl/README.md)

    - The instructions below are the same as those found in the above README

3. **Make `cert.pem` and `key.pem`**

    ```
    openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 99999 -out cert.pem
    ```

4. **Make key store (use the password "password")**

    ```
    openssl pkcs12 -export -in cert.pem -inkey key.pem -out keystore.p12 -name exampleKeyStore
    ```

5. **Make trust store (use the password "password")**

    ```
    keytool -importcert -alias exampleCert -file cert.pem -keystore truststore.jks
    ```

6. **Restart AMPS and Flink**

    - If an example was already ran without setting up the SSL resources, the AMPS instance with the configuration `amps-configs/config.xml` and the Flink cluster may need to be restarted

## Set Up AMPS

The `amps-configs/` directory includes several AMPS configuration files that are used in the examples.
In order to ensure all examples run properly, you will need to start several AMPS instances.
Additionally, if high performance for the examples is desired, you may need to change the Journal Directory.
For example, in `amps-configs/config.xml`, you should change the journal directory `<JournalDirectory>./amps-files/amps-flink/journals</JournalDirectory>` to a faster storage.

1. **Navigate to the parent directory**
    
    ```
    cd path/to/amps-integration-apache-flink
    ```

2. **Modify configurations** if desired

3. **Start AMPS instances**

    - Each instance will need its own terminal

        ```
        ~/AMPS-{amps_version}-Release-Linux/bin/ampServer amps-configs/config.xml
        ~/AMPS-{amps_version}-Release-Linux/bin/ampServer amps-configs/amps1config.xml
        ~/AMPS-{amps_version}-Release-Linux/bin/ampServer amps-configs/amps2config.xml
        ```

## Set Up Flink

Some examples involve parallelism and the use of Stores/BookmarkStores. It may be necessary to change Flink's configuration to ensure the jobs run properly.

1. **Open a new terminal**

    - This terminal will be used to submit the jobs to Flink

2. **Navigate to your Flink directory**

    ```
    cd path/to/flink-{flink_version}
    ```

3. **Modify Flink Configuration**

    - Change "taskmanager.numberOfTaskSlots" and "taskmanager.memory.process.size" in `conf/config.yaml`
        
        ```
        taskmanager:
            numberOfTaskSlots: 5
            memory:
                process:
                    size: 6000m
        ```

4. **Start Flink cluster**

    - Start cluster
        
        ```
        bin/start-cluster.sh
        ```

## Run Example

There are several examples that can be found in the `flink-connector-amps-examples/src/main/java/com/crankuptheamps/flink/example/` directory. In order to run the job, you will need to do the following:

1. **Open a new terminal**

    - This terminal will be used to build the jobs

2. **Navigate to the parent directory**

    ```
    cd path/to/amps-integration-apache-flink
    ```

3. **Build the Flink job**

    - Build the job using the property `-DmainClass=[ExampleClass]` to choose which example to use
    - `SimpleExample.java` is used by default, so this property is necessary to run other examples
    - If the desired class is `VolumeExample.java`, use `-DmainClass=VolumeExample`
        
        ```
        mvn clean package -DmainClass=VolumeExample
        ```
    - The JAR should be `flink-connector-amps-examples/target/flink-connector-amps-examples-{connector_version}.jar`

4. **Submit the job to Flink**

    - Select the terminal that is in the `flink-{flink_version}` directory
    - Submit the job
        
        ```
        bin/flink run path/to/flink-connector-amps-examples/target/flink-connector-amps-examples-{connector_version}.jar
        ```

5. **Monitor the job**

    - Some examples print to the console while others require using the Galvanometer or Flink web UI
    - The Galvanometer (localhost:8085 by default) to monitor AMPS
        - The Galvanometer on ports 8086 and 8087 can be used to monitor the AMPS instances that focus on replication examples
    - The Flink web UI (localhost:8081 by default) to monitor Flink

6. **Cancel the job**
    
    - In the Flink web UI, go to the "Overview" tab, click on the job in the "Running Job List", and cancel the job

**Note:** Some of the examples require messages to already exist in the transaction log. Several examples, such as `CheckpointExample.java`, include methods that can publish a specific amount of messages to AMPS that will be used in the job. Depending on how you want to run the examples, you may need to specify how many messages you want to publish to AMPS before running the Flink job.

## Configuration

In addition to modifying the source files for the examples, the following can be done to configure the examples.

### AMPS Configuration Files

The configurations for the AMPS instances can be changed, and if testing the throughput of the connectors, it may be necessary to change the `JournalDirectory` for the transaction logs.

Several helper classes in the `flink-connector-amps-examples/src/main/java/com/crankuptheamps/flink/example/helper/` directory may need to be changed depending on modifications made to the configuration files. For example, `Constants.java` may need topics/uris changed to match changes in the configuration files.

### Arguments

The following arguments can be used to modify some aspects of a Flink job without having to rebuild the example. Multiple arguments can be used at once. For example, `--publishAmount 0 --parallelism 2` is valid. Examples that do not use an argument will simply ignore the given argument.

- **publishAmount**
    - Used to override the amount of messages published to AMPS before running the Flink job
    - The following can be used to avoid publishing any messages and run the job:

        ```
        bin/flink run path/to/flink-connector-amps-examples/target/flink-connector-amps-examples-{connector_version}.jar --publishAmount 0
        ```
    - The following examples use this argument:
        - AggregationExample
        - AMPSSourceExample
        - BatchExample
        - CheckpointExample
        - CustomDeserializerExample
        - CustomSerializerExample
        - ParallelSourceExample
        - SOWExample
        - TableExample
        - VolumeExample

- **splitAmount**
    - Used to override the amount of splits that should be used in the Flink job
    - Should generally be equal to parallelism
    - The following can be used to create two splits regardless of the parallelism of the job:

        ```
        bin/flink run path/to/flink-connector-amps-examples/target/flink-connector-amps-examples-{connector_version}.jar --splitAmount 2
        ```
    - The following examples use this argument:
        - AMPSSourceExample
        - CheckpointExample
        - ParallelSourceExample
        - VolumeExample

- **parallelism**
    - Used to override the parallelism that should be used in the Flink job
    - The following can be used to set parallelism to two:

        ```
        bin/flink run path/to/flink-connector-amps-examples/target/flink-connector-amps-examples-{connector_version}.jar --parallelism 2
        ```
    - The following examples use this argument:
        - AMPSSinkExample
        - AMPSSourceExample
        - CheckpointExample
        - ParallelSourceExample
        - VolumeExample

