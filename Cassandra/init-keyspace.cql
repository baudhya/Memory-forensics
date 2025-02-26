CREATE KEYSPACE IF NOT EXISTS mykeyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
