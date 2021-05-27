# Quickstart

This quickstart will show how to setup the ScyllaDB Sink Connector against a Dockerized ScyllaDB.


## Preliminary Setup

###Docker Setup
This [link](https://hub.docker.com/r/scylladb/scylla/) provides docker commands to bring up ScyllaDB.

Command to start ScyllaDB docker container:

```
$ docker run --name some-scylla --hostname some-scylla -d scylladb/scylla
```
Running `docker ps` will show you the exposed ports, which should look something like the following:
```

$ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                              NAMES
26cc6d47efe3        replace-with-image-name   "/docker-entrypoint.…"   4 hours ago         Up 23 seconds       0.0.0.0:32777->1883/tcp, 0.0.0.0:32776->9001/tcp   anonymous_my_1
```

### Confluent Platform Installation

If you are new to Confluent then follow this [link](https://www.confluent.io/download) to download the Confluent Platform .


1 - Click on DOWNLOAD FREE under Self managed software.

2 - Click on Zip archive then fill the Email address then Accept the T&C and lastly click on Download Version 5.X.X.

3 - Extract the downloaded file and paste it to the desired location.

4 - Now follow this [link](https://docs.confluent.io/current/quickstart/ce-quickstart.html#ce-quickstart) to complete the installation.


### Manual Installation Of The Connector

For manual installation, navigate to the following github link and clone the repository.

``https://github.com/scylladb/kafka-connect-scylladb``

Follow these steps to build the project:
* Open source code folder in terminal.
* Run the command ``mvn clean install``.
* Run the Integration Tests in an IDE. If tests fail run ``mvn clean install -DskipTests``.

Note: To run Integration Tests there is no need to run Confluent. Use docker-compose.yml file in the github repository and run the following command( it contains images to run kafka and other services):

``docker-compose -f docker-compose.yml up``

After completion of the above steps, a folder by the name of ‘components’ will be created in the target folder of the source code folder.
The Connector jar files are present in ``{source-code-folder}/target/components/packages/[jar-files]``

Create a folder by the name of ScyllaDB-Sink-Connector and copy the jar files in it.
Navigate to your Confluent Platform installation directory and place this folder in {confluent-directory}/share/java.

## Sink Connector

The ScyllaDB sink connector is used to publish records from a Kafka topic into ScyllaDB.

Adding a new connector plugin requires restarting Connect. Use the Confluent CLI to restart Connect:

```
$ confluent local stop && confluent local start
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
```

Check if the kafka-connect-scylladb connector plugin has been installed correctly and picked up by the plugin loader:

```
$ curl -sS localhost:8083/connector-plugins | jq .[].class | grep ScyllaDbSinkConnector
```
Your output should resemble:

```
"io.connect.scylladb.ScyllaDbSinkConnector"
```

#####Connector Configuration

Save these configs in a file *kafka-connect-scylladb.json* and run the following command:

```
{
     "name" : "scylladb-sink-connector",
     "config" : {
       "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
       "tasks.max" : "1",
       "topics" : "topic1,topic2,topic3",
       "scylladb.contact.points" : "scylladb-hosts",
       "scylladb.keyspace" : "test"
       }
}
```

Use this command to load the connector :

```
curl -s -X POST -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors
```

Use the following command to update the configuration of existing connector.

```
curl -s -X PUT -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors/scylladb/config
```

Once the Connector is up and running, use the command ``kafka-avro-console-producer`` to produce records(in AVRO format) into Kafka topic.

Example:

```
kafka-avro-console-producer 
--broker-list localhost:9092 
--topic topic1  
--property parse.key=true 
--property key.schema='{"type":"record",name":"key_schema","fields":[{"name":"id","type":"int"}]}' 
--property "key.separator=$" 
--property value.schema='{"type":"record","name":"value_schema","fields":[{"name":"id","type":"int"},
{"name":"firstName","type":"string"},{"name":"lastName","type":"string"}]}'
{"id":1}${"id":1,"firstName":"first","lastName":"last"}
```

Output upon running the select query in ScyllaDB:
select * from test.topic1;

```
 id | firstname | lastname
 
----+-----------+----------

  1 |     first |     last
  ```


##Modes in ScyllaDB

###Standard

Use this command to load the connector in :

```
curl -s -X POST -H 'Content-Type: application/json' --data @kafka-connect-scylladb.json http://localhost:8083/connectors
```

This example will connect to ScyllaDB instance without authentication.

Select one of the following configuration methods based on how you have deployed |kconnect-long|.
Distributed Mode will the JSON / REST examples. Standalone mode will use the properties based
example.

**Note**: Each json record should consist of a schema and payload.


**Distributed Mode JSON**

```
    {
     "name" : "scylladb-sink-connector",
     "config" : {
       "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
       "tasks.max" : "1",
       "topics" : "topic1,topic2,topic3",
       "scylladb.contact.points" : "scylladb-hosts",
       "scylladb.keyspace" : "test",
       "key.converter" : "org.apache.kafka.connect.json.JsonConverter",
       "value.converter" : "org.apache.kafka.connect.json.JsonConverter"
       "key.converter.schemas.enable" : "true",
       "value.converter.schemas.enable" : "true",
           	 	 	
       "transforms" : "createKey",
       "transforms.createKey.fields" : "[field-you-want-as-primary-key-in-scylla]",
       "transforms.createKey.type" : "org.apache.kafka.connect.transforms.ValueToKey"
     }
   }
```

**Standalone Mode Json**

To load the connector in Standalone mode use:

```
confluent local load scylladb-sink-conector -- -d scylladb-sink-connector.properties
```
Use the following configs:

```
   scylladb.class=io.connect.scylladb.ScyllaDbSinkConnector
   tasks.max=1
   topics=topic1,topic2,topic3
   scylladb.contact.points=cassandra
   scylladb.keyspace=test

   key.converter=org.apache.kafka.connect.json.JsonConverter
   value.converter=org.apache.kafka.connect.json.JsonConverter
   key.converter.schemas.enable=true
   value.converter.schemas.enable=true
   	 	 	
   transforms=createKey
   transforms.createKey.fields=[field-you-want-as-primary-key-in-scylla]
   transforms.createKey.type=org.apache.kafka.connect.transforms.ValueToKey
```

Example:

```
kafka-console-producer --broker-list localhost:9092 --topic sample-topic
>{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"department"}],"payload":{"id":10,"name":"John Doe10","department":"engineering"}}}
```

Run the select query to view the data:

```
Select * from keyspace_name.topic-name;
```

**Note**: To publish records in Avro Format use the following properties:

```
   key.converter=io.confluent.connect.avro.AvroConverter
   key.converter.schema.registry.url=http://localhost:8081
   value.converter=io.confluent.connect.avro.AvroConverter
   value.converter.schema.registry.url=http://localhost:8081
   key.converter.schemas.enable=true
   value.converter.schemas.enable=true
```

----------------------
Authentication
----------------------

This example will connect to ScyllaDB instance with security enabled and username / password authentication.


Select one of the following configuration methods based on how you have deployed |kconnect-long|.
Distributed Mode will the JSON / REST examples. Standalone mode will use the properties based
example.


**Distributed Mode**

```
{
  "name" : "scylladbSinkConnector",
  "config" : {
    "connector.class" : "io.connect.scylladb.ScyllaDbSinkConnector",
    "tasks.max" : "1",
    "topics" : "topic1,topic2,topic3",
    "scylladb.contact.points" : "cassandra",
    "scylladb.keyspace" : "test",
    "scylladb.security.enabled" : "true",
    "scylladb.username" : "example",
    "scylladb.password" : "password",
    **add other properties same as in the above example**
  }
}
```


**Standalone Mode**

```
connector.class=io.connect.scylladb.ScyllaDbSinkConnector
tasks.max=1
topics=topic1,topic2,topic3
scylladb.contact.points=cassandra
scylladb.keyspace=test
scylladb.ssl.enabled=true
scylladb.username=example
scylladb.password=password
```

###Logging

To check logs for the Confluent Platform use:

```
confluent local log <service> -- [<argument>] --path <path-to-confluent>
```
To check logs for Scylla:

```
$ docker logs some-scylla | tail
```
