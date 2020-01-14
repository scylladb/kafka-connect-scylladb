package io.connect.scylladb;

interface ScyllaDbSessionFactory {
    ScyllaDbSession newSession(ScyllaDbSinkConnectorConfig connectorConfig);
}
