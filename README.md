ScyllaDB Sink Connector
========================

The ScyllaDB Sink Connector is a high-speed mechanism for reading records from Kafka and writing to ScyllaDB.

Connector Installation
-------------------------------

Clone the connector from Github repository and refer this [link](./documentation/QUICKSTART.md) for quickstart.

## Prerequisites
The following are required to run the ScyllaDB Sink Connector:
* Kafka Broker: Confluent Platform 3.3.0 or above.
* Connect: Confluent Platform 4.1.0 or above.
* Java 1.8
* ScyllaDB: cqlsh 5.0.1 | Cassandra 3.0.8 | CQL spec 3.3.1 | Native protocol v4


Usage Notes
-----------
The ScyllaDB Sink Connector accepts two data formats from kafka. They are:
* Avro Format 
* JSON with Schema
* JSON without Schema

**Note:** In case of JSON without schema, the table should already be present in the keyspace.

This connector uses the topic name to determine the name of the table to write to. You can change this dynamically by using a
transform like [Regex Router](<https://kafka.apache.org/documentation/#connect_transforms>) to change the topic name.

To run this connector you can you a dockerized ScyllaDB instance. Follow this [link](https://hub.docker.com/r/scylladb/scylla/) for use.


-----------------
Schema Management
-----------------

You can configure this connector to manage the schema on the ScyllaDB cluster. When altering an existing table the key
is ignored. This is to avoid the potential issues around changing a primary key on an existing table. The key schema is used to
generate a primary key for the table when it is created. These fields must also be in the value schema. Data
written to the table is always read from the value from Apache Kafka. This connector uses the topic to determine the name of
the table to write to. This can be changed on the fly by using a transform to change the topic name.

--------------------------
Time To Live (TTL) Support
--------------------------
This connector provides support for TTL by which data can be automatically expired after a specific period.
``TTL`` value is the time to live value for the data. After that particular amount of time, data will be automatically deleted. For example, if the TTL value is set to 100 seconds then data would be automatically deleted after 100 seconds.
To use this feature you have to set ``scylladb.ttl`` config with time(in seconds) for which you want to retain the data. If you don't specify this property then the record will be inserted with default TTL value null, meaning that written data will not expire.

--------------------------------
Offset tracking Support in Kafka
--------------------------------
This connector support two types of offset tracking support.

**Offset stored in ScyllaDB Table**

This is the default behaviour of the connector. Here, the offset is stored in the ScyllaDB table.

**Offset stored in Kafka**

If you want that offset should be managed in kafka then you must specify ``scylladb.offset.storage.table.enable=false``. By default, this property is true (in this case offset will be stored in the ScyllaDB table).

-------------------
Delivery guarantees
-------------------
This connector has at-least-once semantics. In case of a crash or restart, an `INSERT` operation of some rows
might be performed multiple times (at least once). However, `INSERT` operations are idempotent in Scylla, meaning
there won't be any duplicate rows in the destination table.
 
The only time you could see the effect of duplicate `INSERT` operations is if your destination table has 
[Scylla CDC](https://docs.scylladb.com/using-scylla/cdc/) turned on. In the CDC log table you would see duplicate
`INSERT` operations as separate CDC log rows. 

-----------------------
Reporting Kafka Metrics
-----------------------

Refer the following [confluent documentation](https://docs.confluent.io/current/kafka/metrics-reporter.html)
to access kafka related metrics.

-----------------------
Supported Data Types
-----------------------
Currently supported native database types are: ASCII, BIGINT, BLOB, BOOLEAN, DATE, DECIMAL, DOUBLE, 
DURATION, FLOAT, INET, INT, SMALLINT, TEXT, TIME, TIMESTAMP, TIMEUUID, TINYINT, UUID, VARCHAR, VARINT.

COUNTERs are not supported.

Collections, UDTs and Tuples are supported, although UDTs require proper setup.
To ensure connector knows how to handle User Types it is necessary that they are already defined in 
target keyspace when connector starts.

Best way to ensure conformity with specific table schema is to create table beforehand and use 
connector with `TABLE_MANAGE_ENABLED_CONFIG` set to `false`. Connector will try to reasonably 
convert most of the types (e.g. IP address provided as text string should be possible to insert into INET column).