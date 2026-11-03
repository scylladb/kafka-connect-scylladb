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
  and must start with a letter, so names such as `1ks` or `a-b` fail validation unless quoted. Quoted names,
  such as `"MyKs"`, are accepted and keep their case.
* All settings of one topic must name the same `my_ks` and `my_table`.
* `my_ks` must already exist, unless it is ``scylladb.keyspace`` and ``scylladb.keyspace.create.enabled`` is
  true: the connector creates no other keyspace, and validation fails when `my_ks` is missing.
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
  ``__expiration`` must reference a Kafka Timestamp, or a Unix epoch time as an integer (or a string holding one)
  in the unit set by ``expirationUnit``, which it requires; an expiry of ``0``, in either form, means the record
  never expires. Its path may step into nested structs or maps, as in ``__expiration=value.metadata.exp``.
  The connector calculates a per-record TTL from the absolute expiration time. A negative expiry, or a TTL above
  ScyllaDB's maximum of 20 years (630720000 seconds), fails the record: it goes to the errant record reporter under
  ``errors.tolerance``, or, without a reporter, ``behavior.on.error`` decides.
  A mapping cannot contain both ``__ttl`` and ``__expiration``.
  By default, the database internally tracks the write time(timestamp) of records inserted into Kafka. 
  However, this __timestamp feature in the mapping supports the scenario where the Kafka records have an explicit 
  timestamp field that you want to use as a write time for the database record produced by the connector. 
  Eg. "topic.my_topic.my_ks.my_table.mapping": 
  "column1=key.field1, column2=value.field1, __ttl=value.field2, __timestamp=value.field3, column3=header.field1"

  Or, with an absolute expiration time in epoch seconds:
  "topic.my_topic.my_ks.my_table.mapping":
  "column1=key.field1, column2=value.field1, __expiration=value.validUntil, __timestamp=value.field3, column3=header.field1"
  "topic.my_topic.my_ks.my_table.expirationUnit": "SECONDS"

  **Deprecated**: ``__timestamp`` reads only the value, in microseconds, and tombstones use the Kafka record timestamp.
  Use ``scylladb.timestamp.field`` or ``timestampField`` instead; they take precedence over ``__timestamp``.

  A tombstone deletes by the record key alone. Each key field binds to the primary key column of its ``key.``
  entry, or else to the one its ``value.`` namesake maps to, or, when the mapping has no ``key.`` entries, to the
  one of its own name. Each column is bound once, a ``key.`` entry first; other key fields, and ``key.`` entries
  to columns outside the primary key, are ignored. A tombstone that leaves a primary key column of an existing
  table unbound fails; one for a missing table is skipped first.
  
``topic.my_topic.my_ks.my_table.consistencyLevel``

  By using this property we can specify table wide consistencyLevel.

``topic.my_topic.my_ks.my_table.ttlSeconds``

  By using this property we can specify table wide ttl(time-to-live).

``topic.my_topic.my_ks.my_table.expirationOffsetSeconds``

  A non-negative number of seconds added to the timestamp referenced by ``__expiration`` before calculating TTL.
  For example, use ``86400`` to expire a record one day after its expiration timestamp. The default is ``0``.

``topic.my_topic.my_ks.my_table.expirationUnit``

  The unit of an integer ``__expiration`` field, or of a string holding one: ``SECONDS`` or ``MILLISECONDS``.
  It has no default and is required whenever the mapping contains ``__expiration``, since integers read in the
  wrong unit would all be skipped as expired, or fail. A Kafka Timestamp field still needs the setting, which it
  then ignores.

``topic.my_topic.my_ks.my_table.skipExpired``

  What to do with a record whose expiration time, plus ``expirationOffsetSeconds``, is not in the future.
  ``true`` (the default) skips it with a warning, leaving any earlier version of the row as it was. ``false``
  fails it: it goes to the errant record reporter under ``errors.tolerance``, or, without a reporter,
  ``behavior.on.error`` decides.

``topic.my_topic.my_ks.my_table.deletesEnabled``

  By using this property we can specify if tombstone records(records with Kafka value as null) 
  should processed as delete request. It applies to records flagged by ``scylladb.deletes.field`` too.

``topic.my_topic.my_ks.my_table.timestampField``

  Overrides ``scylladb.timestamp.field`` for this topic; an empty value turns the field off, and a ``__timestamp``
  mapping of the topic then still applies. The unit is still ``scylladb.timestamp.unit`` unless
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
  * Valid Values: ``ANY``, ``ONE``, ``TWO``, ``THREE``, ``QUORUM``, ``ALL``, ``LOCAL_QUORUM``, ``EACH_QUORUM``, ``LOCAL_ONE``

  ``SERIAL`` and ``LOCAL_SERIAL`` apply only to conditional updates, which the connector does not write, so
  they fail validation here and in ``topic.my_topic.my_ks.my_table.consistencyLevel``.

``scylladb.deletes.enabled``

  Flag to determine if the connector should process deletes. 
  The Kafka records with kafka record value as null, or flagged by ``scylladb.deletes.field``, will result in
  deletion of ScyllaDB record with the primary key present in Kafka record key.

  * Type: boolean
  * Default: true
  * Importance: high

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
  A record whose field is missing or null fails: it goes to the errant record reporter under ``errors.tolerance``,
  or, without a reporter, ``behavior.on.error`` decides.
  Tombstones have no value, so they need a ``key.`` or ``header.`` field. Prefer ``header.``: a table the
  connector creates without a ``mapping`` puts every key field into the primary key, so with ``key.`` each event
  would get its own row.
  Use ``key.`` only when the table already exists or the field is meant to be part of the key.
  When empty, the Kafka record timestamp is used. It has millisecond precision, so events for one row within the
  same millisecond get equal timestamps and may be applied out of order, with a delete winning a tie.

  ScyllaDB orders the writes to one row by their write timestamps alone; the connector sends them concurrently.
  A timestamp from the source makes redelivered events idempotent: an older write or delete cannot override
  a newer one, except that a replayed write with a TTL (``scylladb.ttl``, ``ttlSeconds`` or ``__ttl``) starts its
  TTL again. Events with equal timestamps are still resolved by ScyllaDB's tie rules (a delete wins over a
  write), so the field orders events correctly only when it changes with every event of a row. Write timestamps
  have microsecond precision, so with ``NANOSECONDS`` events within one microsecond also tie.
  A delete is kept as a tombstone only until ScyllaDB may purge it: ``gc_grace_seconds`` after the delete, or
  after a repair with ``tombstone_gc`` mode ``repair``. An older write replayed after that can bring the row back,
  so keep that window longer than the longest replay delay you expect.
  Create the table before records flow: a delete for a missing table is skipped, so a later write with an older
  timestamp would then create the row.

  * Type: String
  * Importance: Medium
  * Default Value: ""

``scylladb.timestamp.unit``

  The unit of the integer in ``scylladb.timestamp.field``. Fields of the Kafka Connect ``Timestamp`` type ignore it;
  ``Date`` and ``Time`` fields are rejected, since they hold no point in time. A value that does not fit in
  microseconds fails the record; ``NANOSECONDS`` are truncated to microseconds.

  * Type: String
  * Importance: Low
  * Valid Values: ``SECONDS``, ``MILLISECONDS``, ``MICROSECONDS``, ``NANOSECONDS``
  * Default Value: MICROSECONDS

``scylladb.max.retries``

  The number of times a batch is retried after consecutive transient write failures, such as
  ScyllaDB being unreachable or a timeout, before the task fails. 0 fails the task on the first
  one; -1 retries without limit. See [Error handling](#error-handling).

  * Type: Int
  * Importance: Medium
  * Valid Values: [-1,...]
  * Default Value: -1

``scylladb.retry.backoff.ms``

  The time to wait before retrying a batch after a transient write failure. The worker never waits
  past its next offset commit (``offset.flush.interval.ms``).

  * Type: Long
  * Importance: Low
  * Valid Values: [1,...]
  * Default Value: 3000

``scylladb.deletes.field``

  The record field that marks a record with a value as a delete, for sources whose change streams send a delete
  as a record with a payload rather than as a tombstone. Written like ``scylladb.timestamp.field``:
  ``key.<field>``, ``value.<field>`` or ``header.<name>``, with nested fields separated by dots.
  A record whose field holds one of ``scylladb.deletes.values`` deletes the row with the primary key in the record
  key, exactly like a tombstone: it needs ``scylladb.deletes.enabled``, binds only the key, and uses the same
  write timestamp as a write. A record whose field is missing, null or holds another value is written, and a
  missing or null field is logged as a warning once per topic and task; one whose field holds anything but a string,
  number or boolean fails, as does one whose path steps into something that is not a struct or map.
  When empty, only tombstones are deletes.

  For example, when the change metadata of each record names the operation and its last-update time in
  milliseconds:

      scylladb.deletes.field=value.metadata.msg
      scylladb.deletes.values=delete
      scylladb.timestamp.field=value.metadata.lut
      scylladb.timestamp.unit=MILLISECONDS

  A delete binds the primary key from the record key exactly as a tombstone does; see ``mapping``. Take the write
  timestamp from the source, as above: change streams redeliver and reorder events, and only then does a stale
  write lose to a later delete. A delete is kept as a tombstone only until ScyllaDB may purge it. Under the default
  ``tombstone_gc`` mode ``repair``, that is after the first repair that runs ``propagation_delay_in_seconds`` (one
  hour by default) after the delete, or right away at replication factor 1; under mode ``timeout``, it is
  ``gc_grace_seconds`` after the delete. An older write replayed after that can bring the row back, so keep that
  delay longer than the longest replay delay you expect.

  * Type: String
  * Importance: Medium
  * Default Value: ""

``scylladb.deletes.values``

  The values of ``scylladb.deletes.field`` that mark a delete, compared case-sensitively with the field's text
  form, for example ``delete``, or ``true`` for a boolean flag. Required when ``scylladb.deletes.field`` is set.

  * Type: List
  * Importance: Medium
  * Default Value: ""

### ScyllaDB

``behavior.on.error``

How a record error, a record that ScyllaDB rejects or that cannot be converted, is handled.
Transient errors are retried and fatal errors fail the task whatever this setting is. Must be
configured to one of the following:

``fail``

The task fails with ConnectException at the first record error. Records after one that cannot be
converted are not written, but records after one ScyllaDB rejects may be, as writes run concurrently.
No offsets are committed, so the batch is redelivered.

``ignore``

The record is skipped.

``log``

The error is logged and the record is skipped.

* Type: string
* Default: FAIL
* Valid Values: [FAIL, LOG, IGNORE]
* Importance: medium

When the worker provides an errant record reporter (``errors.deadletterqueue.topic.name`` or
``errors.log.enable`` is set, see [Error handling](#error-handling)), failed records are reported to
it instead, ``errors.tolerance`` applies, and this setting is not used.


### Error handling

The connector tells three kinds of failures apart:

* **Record errors**: ScyllaDB rejects the record itself (for example an empty partition key), or
  the record cannot be converted. Retrying cannot help. When the worker provides an errant record
  reporter, the record is reported to it and ``errors.tolerance`` decides: with ``none`` (the Kafka
  Connect default) the task fails, with ``all`` the task continues. The record goes to the dead
  letter queue when ``errors.deadletterqueue.topic.name`` is set; with only ``errors.log.enable``
  it is logged. Without a reporter, ``behavior.on.error`` decides.
* **Transient errors**: ScyllaDB is unreachable, times out, is overloaded, rate-limits a partition,
  or a replica fails the write. The task waits ``scylladb.retry.backoff.ms`` and the batch is delivered again, up to
  ``scylladb.max.retries`` times in a row; the next failure fails the task. By default it retries
  without limit, so an outage blocks the task until ScyllaDB is back, with no data loss. A batch is
  retried as a whole: its record errors are handled once the rest of it is written. A write that a
  replica rejects for exceeding a large data threshold reaches the connector as an ordinary write
  failure, so it is retried too; set ``scylladb.max.retries`` to keep such a record from blocking
  the task.
* **Fatal errors**: rejected credentials or permissions, an invalid query or configuration (such as
  a missing keyspace, table or column, a table whose primary key differs from the mapping, or a
  write consistency level the cluster forbids), a server or protocol error, or an unexpected
  error. The task fails. A keyspace, table or column dropped while the task runs fails it too;
  with ``scylladb.table.manage.enabled``, restarting the task recreates the table or column. A
  delete from a missing table is skipped with a warning.

With ``scylladb.offset.storage.table.enable=true`` (the default) the task reads its offsets from
ScyllaDB when it starts, so an outage at that moment fails the task. Writes to the offset table are
retried by the worker and do not count against ``scylladb.max.retries``; bounding them is tracked in
[#294](https://github.com/scylladb/kafka-connect-scylladb/issues/294).

To send record errors to a dead letter queue, use the standard Kafka Connect settings:

```
errors.tolerance=all
errors.deadletterqueue.topic.name=scylladb-sink-dlq
errors.deadletterqueue.context.headers.enable=true
```

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
