CREATE KEYSPACE IF NOT EXISTS lab
  WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
