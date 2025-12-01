# Example setups
Below are examples of non-trivial connector configurations.   
For simpler step-by-step instructions with environment setup check out [quickstart](QUICKSTART.md) first.


## One topic to many tables

This can be achieved by running multiple instances of the connector.

#### Environment
Running Kafka cluster and dockerized ScyllaDB (contact point `172.17.0.2`)
Test data generated using [Datagen Source Connector](https://www.confluent.io/hub/confluentinc/kafka-connect-datagen)
with following configuration:
```
name = DatagenConnectorExample_1
connector.class = io.confluent.kafka.connect.datagen.DatagenConnector
kafka.topic = usersTopic
quickstart = users
```

#### Connectors configuration
We will use 2 connectors for this example.

Connector1.properties:
```
name = ScyllaDbSinkConnectorExample_1
connector.class = io.connect.scylladb.ScyllaDbSinkConnector
transforms = createKey
topics = usersTopic
transforms.createKey.type = org.apache.kafka.connect.transforms.ValueToKey
transforms.createKey.fields = userid
scylladb.contact.points = 172.17.0.2
scylladb.consistency.level = ONE
scylladb.keyspace = example_ks
scylladb.keyspace.replication.factor = 1
scylladb.offset.storage.table = kafka_connect_offsets
```
Connector2.properties:
```
name = ScyllaDbSinkConnectorExample_2
connector.class = io.connect.scylladb.ScyllaDbSinkConnector
transforms = createKey, ChangeTopic
topics = usersTopic
transforms.createKey.type = org.apache.kafka.connect.transforms.ValueToKey
transforms.createKey.fields = userid
transforms.ChangeTopic.type = org.apache.kafka.connect.transforms.RegexRouter
transforms.ChangeTopic.regex = usersTopic
transforms.ChangeTopic.replacement = ChangedTopic
scylladb.contact.points = 172.17.0.2
scylladb.consistency.level = ONE
scylladb.keyspace = example_ks
scylladb.keyspace.replication.factor = 1
scylladb.offset.storage.table = kafka_connect_offsets_2
topic.ChangedTopic.example_ks.ChangedTopic.mapping = mappedUserIdCol=key.userid,mappedGenderCol=value.gender
```
This setup results in creation of 4 tables in `example_ks` keyspace. Two different for keeping offsets and two different for data.

Connector1 creates `userstopic` which should look like table below. Keeps its offsets in `kafka_connect_offsets`.
<pre> <font color="#C01C28"><b>userid</b></font> | <font color="#A347BA"><b>gender</b></font> | <font color="#A347BA"><b>regionid</b></font> | <font color="#A347BA"><b>registertime</b></font>
--------+--------+----------+---------------
 <font color="#A2734C"><b>User_3</b></font> |   <font color="#A2734C"><b>MALE</b></font> | <font color="#A2734C"><b>Region_3</b></font> | <font color="#26A269"><b>1497171901434</b></font>
 <font color="#A2734C"><b>User_1</b></font> |  <font color="#A2734C"><b>OTHER</b></font> | <font color="#A2734C"><b>Region_2</b></font> | <font color="#26A269"><b>1515602353163</b></font>
 <font color="#A2734C"><b>User_6</b></font> |  <font color="#A2734C"><b>OTHER</b></font> | <font color="#A2734C"><b>Region_3</b></font> | <font color="#26A269"><b>1512008940490</b></font>
 <font color="#A2734C"><b>User_7</b></font> |   <font color="#A2734C"><b>MALE</b></font> | <font color="#A2734C"><b>Region_7</b></font> | <font color="#26A269"><b>1507294138815</b></font>
 <font color="#A2734C"><b>User_2</b></font> | <font color="#A2734C"><b>FEMALE</b></font> | <font color="#A2734C"><b>Region_2</b></font> | <font color="#26A269"><b>1493737097490</b></font>
</pre>
Connector2 uses RegexRouter SMT to change topic name to `changedtopic`. This results in creation of `changedtopic` table. Additionally it makes use of custom mapping property (`topic.ChangedTopic.example_ks...`) to define a different structure for this table.  
This results in following:
<pre>cqlsh&gt; SELECT * FROM example_ks.changedtopic 
   ... ;

 <font color="#C01C28"><b>mappedUserIdCol</b></font> | <font color="#A347BA"><b>mappedGenderCol</b></font>
-----------------+-----------------
          <font color="#A2734C"><b>User_3</b></font> |           <font color="#A2734C"><b>OTHER</b></font>
          <font color="#A2734C"><b>User_1</b></font> |           <font color="#A2734C"><b>OTHER</b></font>
          <font color="#A2734C"><b>User_6</b></font> |           <font color="#A2734C"><b>OTHER</b></font>
          <font color="#A2734C"><b>User_7</b></font> |            <font color="#A2734C"><b>MALE</b></font>
</pre>