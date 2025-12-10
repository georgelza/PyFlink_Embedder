
-- Apache Flink JDBC Catalog Datastore

-- psql -h localhost -p 5432 -U dbadmin -d flink_catalog

GRANT ALL PRIVILEGES ON DATABASE flink_catalog TO dbadmin;

-- Schema that will house our Flink / Paimon JDBC catalogs
CREATE SCHEMA IF NOT EXISTS paimon_catalog AUTHORIZATION dbadmin;

-- -- Grant permissions to the catalog user
GRANT ALL PRIVILEGES ON SCHEMA paimon_catalog TO dbadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA paimon_catalog TO dbadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA paimon_catalog TO dbadmin;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA paimon_catalog GRANT ALL ON TABLES TO dbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA paimon_catalog GRANT ALL ON SEQUENCES TO dbadmin;

COMMENT ON SCHEMA paimon_catalog IS 'Flink / Paimon JDBC Catalog Storage';


-- Apache Polaris Catalog Datastore

-- psql -h localhost -p 5432 -U dbadmin -d findept

CREATE DATABASE findept;
GRANT ALL PRIVILEGES ON DATABASE findept TO dbadmin;
