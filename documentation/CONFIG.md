# ScyllaDB Sink Connector

Configuration Properties
------------------------

To use this connector, specify the name of the connector class in the ``connector.class`` configuration property.

    connector.class=io.connect.scylladb.ScyllaDbSinkConnector

Connector-specific configuration properties are described below.

### Connection

``scylladb.contact.points``

  The ScyllaDB hosts to connect to. Scylla nodes use this list of hosts to find each other and learn the topology of the ring. 
  You must change this if you are running multiple nodes.
  It's essential to put at least 2 hosts in case of bigger cluster, since if first host is down, 
  it will contact second one and get the state of the cluster from it.
  Eg. When using the docker image, connect to the host it uses. 
  To connect to private Scylla nodes, provide a JSON string having all internal private network address:port mapped to
  an external network address:port as key value pairs. Need to pass it as 
  {\"private_host1:port1\",\"public_host1:port1\",\"private_host2:port2\",\"public_host2:port2\", ...}
  Eg. {\"10.0.24.69:9042\": \"sl-eu-lon-2-portal.3.dblayer.com:15227\", \"10.0.24.71:9042\": \"sl-eu-lon-2-portal.2.dblayer.com:15229\", \"10.0.24.70:9042\": \"sl-eu-lon-2-portal.1.dblayer.com:15228\"}
  
  * Type: List
  * Importance: High
  * Default Value: [localhost]
  
``scylladb.port``
 
  The port the ScyllaDB hosts are listening on.
  Eg. When using a docker image, connect to the port it uses(use docker ps)
   
  * Type: Int
  * Importance: Medium
  * Default Value: 9042
  * Valid Values: ValidPort{start=1, end=65535}
  
``scylladb.loadbalancing.localdc``

  The case-sensitive Data Center name local to the machine on which the connector is running. 
  It is a recommended configuration if we have more than one DC.

  * Type: string
  * Default: ""
  * Importance: high

``scylladb.security.enabled``
 
  To enable security while loading the sink connector and connecting to ScyllaDB.
  
  * Type: Boolean
  * Importance: High
  * Default Value: false
  
``scylladb.username``
  
  The username to connect to ScyllaDB with. Set ``scylladb.security.enable = true`` to use this config.
  
  * Type: String
  * Importance: High
  * Default Value: cassandra
  
``scylladb.password``
  
  The password to connect to ScyllaDB with. Set ``scylladb.security.enable = true`` to use this config.
  
  * Type: Password
  * Importance: High
  * Default Value: cassandra
  
``scylladb.compression``
  
  Compression algorithm to use when connecting to ScyllaDB.
  
  * Type: string
  * Default: NONE
  * Valid Values: [NONE, SNAPPY, LZ4]
  * Importance: low
  
``scylladb.ssl.enabled``
  
  Flag to determine if SSL is enabled when connecting to ScyllaDB.
  
  * Type: boolean
  * Default: false
  * Importance: high
  
### SSL

``scylladb.ssl.truststore.path``

  Path to the Java Truststore.

  * Type: string
  * Default: ""
  * Importance: medium

``scylladb.ssl.truststore.password``

  Password to open the Java Truststore with.

  * Type: password
  * Default: [hidden]
  * Importance: medium

``scylladb.ssl.provider``

  The SSL Provider to use when connecting to ScyllaDB.

  * Type: string
  * Default: JDK
  * Valid Values: [JDK, OPENSSL, OPENSSL_REFCNT]
  * Importance: low

### Keyspace

**Note**: Both keyspace and table names consist of only alphanumeric characters, 
cannot be empty and are limited in size to 48 characters (that limit exists 
mostly to avoid filenames, which may include the keyspace and table name, 
to go over the limits of certain file systems). By default, keyspace and table names 
are case insensitive (myTable is equivalent to mytable) but case sensitivity 
can be forced by using double-quotes ("myTable" is different from mytable).

``scylladb.keyspace``

  The keyspace to write to. This keyspace is like a database in the ScyllaDB cluster.
  * Type: String
  * Importance: High

``scylladb.keyspace.create.enabled``

  Flag to determine if the keyspace should be created if it does not exist.
  **Note**: Error if a new keyspace has to be created and the config is false.

  * Type: Boolean
  * Importance: High
  * Default Value: true

``scylladb.keyspace.replication.factor``
    
  The replication factor to use if a keyspace is created by the connector. 
  The Replication Factor (RF) is equivalent to the number of nodes where data (rows and partitions) 
  are replicated. Data is replicated to multiple (RF=N) nodes
  
  * Type: int
  * Default: 3
  * Valid Values: [1,...]
  * Importance: high

### Table

``scylladb.table.manage.enabled``

  Flag to determine if the connector should manage the table.

  * Type: Boolean
  * Importance: High
  * Default Value: true
  
``scylladb.table.create.compression.algorithm``
  
  Compression algorithm to use when the table is created.

  * Type: string
  * Default: NONE
  * Valid Values: [NONE, SNAPPY, LZ4, DEFLATE]
  * Importance: medium

``scylladb.offset.storage.table``

  The table within the ScyllaDB keyspace to store the offsets that have been read from Apache Kafka.

  * Type: String
  * Importance: Low
  * Default: kafka_connect_offsets
  
### Topic to Table

These configurations can be specified for multiple Kafka topics from which records are being processed. 
Also, these topic level configurations will override the behavior of Connector level configurations such as 
``scylladb.consistency.level``, ``scylladb.deletes.enabled``, ``scylladb.deletes.field``,
``scylladb.deletes.values``, ``scylladb.ttl``, ``scylladb.timestamp.field`` and ``scylladb.timestamp.unit``

Records from `my_topic` are written to the table `my_table` in the keyspace `my_ks`:

* `my_topic` is the topic name as seen in a SinkRecord, after all Single Message Transforms. It may contain
  `.` and `-`.
* `my_ks` and `my_table` are CQL names and cannot contain `.`. Unquoted names are case-insensitive, as in CQL,
  and other names, such as `1ks` or `a-b`, fail validation.
* All settings of one topic must name the same `my_ks` and `my_table`.
* `my_ks` must already exist. The connector creates only ``scylladb.keyspace``, when
  ``scylladb.keyspace.create.enabled`` is set, and validation fails when `my_ks` is missing.
* Topics without these settings are written to ``scylladb.keyspace``, to a table named after the topic
  with `.` and `-` replaced by `_`.

**Note**: In earlier versions the `my_ks` and `my_table` parts were ignored: records were always written to
``scylladb.keyspace``, to the table named after the topic. Settings for topics containing `.` or `-` were
ignored unless written with the sanitized topic name (for example `topic.my_topic.ks.table.ttlSeconds` for
the topic `my.topic`). That sanitized form is still accepted, with a deprecation warning,
unless the sanitized name is itself listed in ``topics``. It is not accepted under ``topics.regex``, where
the sanitized name may be another subscribed topic.

``topic.<my_topic>.<my_ks>.<my_table>.mapping``

  For mapping topic and fields from Kafka record's key, value and headers to ScyllaDB table and its columns.
 `my_topic` should refer to the topic name as seen in a SinkRecord passed to the Connector - which means after all Single Message Transforms.   
  For example if you're using RegexRouter to change the topic name from `top1` to `top2` you would use `topic.top2.ks.table.mapping=...` property.
  
  **Note**: Ensure that the data type of the Kafka record's fields are compatible with the data type of the ScyllaDB column.
  In the Kafka topic mapping, you can optionally specify fields that control the ttl (time-to-live), absolute
  expiration time, and write timestamp of the record using the special properties ``__ttl``, ``__expiration``,
  and ``__timestamp``.
  ``__ttl`` must reference an Integer field containing a relative lifetime in seconds.
  ``__expiration`` must reference a Kafka Timestamp or an integer Unix epoch time in the unit set by
  ``expirationUnit``, which it requires; an expiry of ``0``, in either form, means the record never expires. Its
  path may step into nested structs or maps, as in ``__expiration=value.metadata.exp``.
  The connector calculates a per-record TTL from the absolute expiration time.
  A mapping cannot contain both ``__ttl`` and ``__expiration``.
  By default, the database internally tracks the write time(timestamp) of records inserted into Kafka. 
  However, this __timestamp feature in the mapping supports the scenario where the Kafka records have an explicit 
  timestamp field that you want to use as a write time for the database record produced by the connector. 
  Eg. "topic.my_topic.my_ks.my_table.mapping": 
  "column1=key.field1, column2=value.field1, __expiration=value.validUntil, __timestamp=value.field3, column3=header.field1"

  **Deprecated**: ``__timestamp`` reads only the value, in microseconds, and deletes use the Kafka record timestamp.
  Use ``scylladb.timestamp.field`` or ``timestampField`` instead; they take precedence over ``__timestamp``.
  
``topic.my_topic.my_ks.my_table.consistencyLevel``

  By using this property we can specify table wide consistencyLevel.

``topic.my_topic.my_ks.my_table.ttlSeconds``

  By using this property we can specify table wide ttl(time-to-live).

``topic.my_topic.my_ks.my_table.expirationOffsetSeconds``

  A non-negative number of seconds added to the timestamp referenced by ``__expiration`` before calculating TTL.
  For example, use ``86400`` to expire a record one day after its expiration timestamp. The default is ``0``.

``topic.my_topic.my_ks.my_table.expirationUnit``

  The unit of an integer ``__expiration`` field: ``SECONDS`` or ``MILLISECONDS``. It has no default and is
  required whenever the mapping contains ``__expiration``, since integers read in the wrong unit would all be
  skipped as expired, or fail. A Kafka Timestamp field still needs the setting, which it then ignores.

``topic.my_topic.my_ks.my_table.skipExpired``

  What to do with a record whose expiration time, plus ``expirationOffsetSeconds``, is not in the future.
  ``true`` (the default) skips it with a warning, leaving any earlier version of the row as it was. ``false``
  fails it, and its processing follows the configured ``behavior.on.error`` policy.

``topic.my_topic.my_ks.my_table.deletesEnabled``

  By using this property we can specify if tombstone records(records with Kafka value as null) 
  should processed as delete request. It applies to records flagged by ``scylladb.deletes.field`` too.

``topic.my_topic.my_ks.my_table.timestampField``

  Overrides ``scylladb.timestamp.field`` for this topic. The unit is still ``scylladb.timestamp.unit`` unless
  ``timestampUnit`` is set for the topic too.

``topic.my_topic.my_ks.my_table.timestampUnit``

  Overrides ``scylladb.timestamp.unit`` for this topic, whether the field comes from ``timestampField`` or
  ``scylladb.timestamp.field``.

``topic.my_topic.my_ks.my_table.deletesField``

  Overrides ``scylladb.deletes.field`` for this topic. The values are still ``scylladb.deletes.values`` unless
  ``deletesValues`` is set for the topic too.

``topic.my_topic.my_ks.my_table.deletesValues``

  Overrides ``scylladb.deletes.values`` for this topic, as a comma-separated list.


### Write

``scylladb.consistency.level``

  The requested consistency level to use when writing to ScyllaDB. The Consistency Level (CL) determines how many replicas in a cluster that must acknowledge read or write operations before it is considered successful.

  * Type: String
  * Importance: High
  * Default Value: LOCAL_QUORUM
  * Valid Values: ``ANY``, ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``, ``LOCAL_QUORUM``, ``EACH_QUORUM``, ``SERIAL``, ``LOCAL_SERIAL``, ``LOCAL_ONE``

``scylladb.deletes.enabled``

  Flag to determine if the connector should process deletes. 
  The Kafka records with kafka record value as null, or flagged by ``scylladb.deletes.field``, will result in
  deletion of ScyllaDB record with the primary key present in Kafka record key.

  * Type: boolean
  * Default: true
  * Importance: high

``scylladb.deletes.field``

  The record field that marks a record with a value as a delete, for sources whose change streams send a delete
  as a record with a payload rather than as a tombstone. Written like ``scylladb.timestamp.field``:
  ``key.<field>``, ``value.<field>`` or ``header.<name>``, with nested fields separated by dots.
  A record whose field holds one of ``scylladb.deletes.values`` deletes the row with the primary key in the record
  key, exactly like a tombstone: it needs ``scylladb.deletes.enabled``, binds only the key, and uses the same
  write timestamp as a write. A record whose field is missing, null or holds another value is written, and a
  missing or null field is logged as a warning once per topic; one whose field holds anything but a string,
  number or boolean fails.
  When empty, only tombstones are deletes.

  For example, when the change metadata of each record names the operation and its last-update time in
  milliseconds:

      scylladb.deletes.field=value.metadata.msg
      scylladb.deletes.values=delete
      scylladb.timestamp.field=value.metadata.lut
      scylladb.timestamp.unit=MILLISECONDS

  A delete binds only the primary key. With a topic ``mapping``, it binds the columns of the ``key.`` entries;
  without such entries, each key field binds to the column its ``value.`` namesake maps to, or else to the column
  of its own name. Either way, the bound columns must be exactly the primary key. Take the write timestamp from the source, as above: change streams redeliver and reorder events,
  and only then does a stale write lose to a later delete. A delete is kept as a tombstone only for the table's
  ``gc_grace_seconds``; a write replayed after that can bring the row back.

  * Type: String
  * Importance: Medium
  * Default Value: ""

``scylladb.deletes.values``

  The values of ``scylladb.deletes.field`` that mark a delete, compared case-sensitively with the field's text
  form, for example ``delete``, or ``true`` for a boolean flag. Required when ``scylladb.deletes.field`` is set.

  * Type: List
  * Importance: Medium
  * Default Value: ""

``scylladb.execute.timeout.ms``

  The timeout for executing a ScyllaDB statement.

  * Type: Long
  * Importance: Low
  * Valid Values: [0,...]
  * Default Value: 30000

``scylladb.ttl``

  The retention period for the data in ScyllaDB. After this interval elapses, ScyllaDB will remove these records. If this configuration is not provided, the Sink Connector will perform insert operations in ScyllaDB  without TTL setting.

  * Type: Int
  * Importance: Medium
  * Default Value: null

``scylladb.offset.storage.table.enable``

  If true, Kafka consumer offsets will be additionally stored in ScyllaDB table. If false, connector will skip writing offset
  information into ScyllaDB.

  * Type: Boolean
  * Importance: Medium
  * Default Value: True

``scylladb.timestamp.field``

  The record field to take the write timestamp (CQL ``USING TIMESTAMP``) from, for both writes and deletes.
  Written as ``key.<field>``, ``value.<field>`` or ``header.<name>``; nested fields are separated by dots,
  for example ``value.metadata.lut`` for a last-update time carried in the change metadata of the record.
  A header name may itself contain dots: the longest name that matches a header is used.
  A record whose field is missing or null fails, and ``behavior.on.error`` decides what happens next.
  Tombstones have no value, so deletes need a ``key.`` or ``header.`` field.
  When empty, the Kafka record timestamp is used. It has millisecond precision, so events for one row within the
  same millisecond get equal timestamps and may be applied out of order, with a delete winning a tie.

  ScyllaDB orders the writes to one row by their write timestamps alone; the connector sends them concurrently.
  A timestamp from the source makes redelivered events idempotent: an older write or delete cannot override
  a newer one. Events with equal timestamps are still resolved by ScyllaDB's tie rules (a delete wins over a
  write), so the field orders events correctly only when it changes with every event of a row.

  * Type: String
  * Importance: Medium
  * Default Value: ""

``scylladb.timestamp.unit``

  The unit of the integer in ``scylladb.timestamp.field``. Fields of the Kafka Connect ``Timestamp`` type ignore it;
  ``Date`` and ``Time`` fields are rejected, since they hold no point in time. A value that does not fit in
  microseconds fails the record.

  * Type: String
  * Importance: Low
  * Valid Values: ``SECONDS``, ``MILLISECONDS``, ``MICROSECONDS``, ``NANOSECONDS``
  * Default Value: MICROSECONDS

### ScyllaDB

``behavior.on.error``

Error handling behavior setting. Must be configured to one of the following:

``fail``

The Connector throws ConnectException and stops processing records when an error occurs while processing or inserting records into ScyllaDB.

``ignore``

Continues to process next set of records when error occurs while processing or inserting records into ScyllaDB.
  
``log``

Logs the error via connect-reporter when an error occurs while processing or inserting records into ScyllaDB and continues to process next set of records, available in the kafka topics.

* Type: string
* Default: FAIL
* Valid Values: [FAIL, LOG, IGNORE]
* Importance: medium


### Confluent Platform Configurations.

``tasks.max``

The maximum number of tasks to use for the connector that helps in parallelism.

* Type:int
* Default: 1
* Importance: high

``topics``

The name of the topics to consume data from and write to ScyllaDB.

* Type: list
* Importance: high

``confluent.topic.bootstrap.servers``

 A list of host/port pairs to use for establishing the initial connection to the Kafka cluster used for licensing. All servers in the cluster will be discovered from the initial connection. This list should be in the form <code>host1:port1,host2:port2,…</code>. Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down).

* Type: list
* Default: localhost:9092
* Importance: high
