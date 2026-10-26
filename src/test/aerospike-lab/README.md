# Aerospike CDC lab

Runs the whole change-data-capture chain on one machine:

```
Aerospike Enterprise --XDR--> Aerospike Kafka outbound connector --> Kafka --> Connect + ScyllaDB sink --> ScyllaDB
```

Use it to capture real Aerospike messages and to test the sink against them.

The Aerospike server uses the evaluation feature key that ships in its image.
That key allows a single node only and is meant for testing. The outbound
connector needs no feature key of its own.

## Prerequisites

- Docker with Compose v2.
- The sink package, built from the repository root:

  ```sh
  mvn -DskipTests package
  ```

  `lab.sh up` copies the package for the current project version to `.plugin/`
  and mounts it on the Connect plugin path. After rebuilding the sink, run
  `./lab.sh up` again: it restarts Connect so the new package is loaded.

## Start

```sh
./lab.sh up
```

This starts every service and waits until it is running, or healthy where it
has a healthcheck. It then creates the `lab` keyspace and registers one sink
connector per topic, from `connect/scylladb-sink-<name>.json`. A failing
connector does not stop the other.

Image versions are set in `.env`. `FORMAT` picks the outbound message format:

| `FORMAT` | Topic | Sink connector |
|---|---|---|
| `json` (default) | `aerospike_json` | `scylladb-sink-json` |
| `flat-json` | `aerospike_flatjson` | `scylladb-sink-flatjson` |

The `avro` profile starts a schema registry for the kafka-avro format, but no
outbound config uses it yet.

To switch format without restarting the rest of the lab:

```sh
FORMAT=flat-json docker compose up -d --wait outbound
```

## Generate changes

```sh
./lab.sh write [pk]    # insert test.users with a stored user key
./lab.sh delete [pk]   # durable delete
docker compose run --rm -T tools aql -h aerospike -c "<aql statements>"
```

XDR ships the latest version of a record, not every change. Updates to the
same key in quick succession arrive as one message. Leave a few seconds between
steps when every step should produce its own message.

## Capture

```sh
./lab.sh capture aerospike_json [dir]
```

This writes every message of the topic to `<dir>/<topic>.txt`, `captures/` by
default. A relative `dir` is resolved against the current directory, so from
the repository root this writes to `src/test/resources/aerospike/`:

```sh
src/test/aerospike-lab/lab.sh capture aerospike_json src/test/resources/aerospike
```

Each line has the timestamp, partition, offset, headers, key and value,
separated by ` | `. If the topic is missing or Kafka is unreachable, the command
fails and keeps the previous file. Binary formats such as kafka-avro and
MessagePack need a different capture method.

## Inspect

```sh
./lab.sh status                                   # sink connectors and task status
docker compose logs outbound                      # outbound connector
docker compose exec scylla cqlsh                  # ScyllaDB
docker compose run --rm -T tools asadm -h aerospike -e info
```

## Tear down

```sh
./lab.sh down
```

This removes all containers and volumes.
