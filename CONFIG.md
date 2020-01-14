#ScyllaDB Sink Connector

Configuration Properties
------------------------

To use this connector, specify the name of the connector class in the ``connector.class`` configuration property.

    connector.class=io.connect.scylladb.ScyllaDbSinkConnector

Connector-specific configuration properties are described below.

###Connection

``scylladb.contact.points``

  The ScyllaDB hosts to connect to.
  Eg. When using the docker image , connect to the host it uses.
  
  * Type: List
  * Importance: High
  * Default Value: [localhost]
  
 ``scylladb.port``
 
  The port the ScyllaDB hosts are listening on.
  Eg. When using a docker image, connect to the port it uses(use docker ps )
   
  * Type: Int
  * Importance: Medium
  * Default Value: 9042
  * Valid Values: ValidPort{start=1, end=65535}

 ``scylladb.security.enabled``
 
  To enable security while loading the sink connector and connecting to ScyllaDB.
  
  * Type: Boolean
  * Importance: High
  * Default Value: False
  
  ``scylladb.username``
  
  The username to connect to ScyllaDB with. Set scylladb.security.enable = true to use this config.
  * Type: String
  * Importance: High
  * Default Value: cassandra
  
  ``scylladb.password``
  
  The password to connect to ScyllaDB with.
  
  * Type: Password
  * Importance: High
  * Default Value: cassandra

### Keyspace


``scylladb.keyspace``

  The keyspace to write to.This keyspace is like a database in the ScyllaDB cluster.
  * Type: String
  * Importance: High


``scylladb.keyspace.create.enabled``

  Flag to determine if the keyspace should be created if it does not exist.
  **Note**: Error if a new keyspace has to be created and the config is false.

  * Type: Boolean
  * Importance: High
  * Default Value: true


###Table

``scylladb.table.manage.enabled``

  Flag to determine if the connector should manage the table.

  * Type: Boolean
  * Importance: High
  * Default Value: true

``scylladb.offset.storage.table``

  The table within the ScyllaDB keyspace to store the offsets that have been read from Apache Kafka.
  This is used to enable exactly once delivery to ScyllaDB.

  * Type: String
  * Importance: Low
  * Default: kafka_connect_offsets

###Write

``scylladb.consistency.level``

  The requested consistency level to use when writing to ScyllaDB.

  * Type: String
  * Importance: High
  * Default Value: LOCAL_QUORUM
  * Valid Values: ``ANY``, ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``, ``LOCAL_QUORUM``, ``EACH_QUORUM``, ``SERIAL``, ``LOCAL_SERIAL``, ``LOCAL_ONE``

``scylladb.execute.timeout.ms``

  The timeout for executing a ScyllaDB statement.

  * Type: Long
  * Importance: Low
  * Default Value: 30000

``scylladb.ttl``

  The retention period for the data in ScyllaDB. After this interval elapses, ScyllaDB will remove these records.

  * Type: Int
  * Importance: Medium
  * Default Value: null

``scylladb.offset.storage.table.enable``

  If true, Kafka consumer offsets will be stored in ScyllaDB table. If false, connector will skip writing offset 
  information into ScyllaDB (this might imply duplicate writes into ScyllaDB when a task restarts).

* Type: Boolean
* Importance: Medium
* Default Value: True


###Confluent Platform Configurations.

``tasks.max``

The maximum number of tasks to use for the connector that helps in parallelism.

* Type:int
* Importance: high

``topics``

The name of the topics to consume data from and write to ScyllaDB.

* Type: list
* Importance: high

``confluent.topic.bootstrap.servers``

 A list of host/port pairs to use for establishing the initial connection to the Kafka cluster used for licensing. All servers in the cluster will be discovered from the initial connection. This list should be in the form <code>host1:port1,host2:port2,…</code>. Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down).

* Type: list
* Importance: high

------------------------
      

