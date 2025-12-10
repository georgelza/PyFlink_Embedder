
# Thinking is we will consume from PostgreSQL sources, then use a PyFlink to flatten the structure, outputting to a Fluss table.
# then reading new Fluss table, calculate embeddings, output to another Fluss table/s. 
# This in turn will be configured with Lakehouse tiering into Paimon table on MinIO S3

USE CATALOG default_catalog;
  
-- Inbound from PostgreSQL via CDC Process
CREATE CATALOG c_cdcsource WITH 
    ('type'='generic_in_memory'); 


CREATE DATABASE IF NOT EXISTS c_cdcsource.demog;  

-- Paimon based Catalog stored inside PostgreSQL database using JDBC interface
-------------------------------------------------------------------------------------------------------------------------
-- server: postgrescat
-- db: flink_catalog
-- schema: paimon_catalog
CREATE CATALOG c_paimon WITH (
     'type'                          = 'paimon'
    ,'metastore'                     = 'jdbc'                       -- JDBC Based Catalog
    ,'catalog-key'                   = 'jdbc'
    -- JDBC connection to PostgreSQL for persistence
    ,'uri'                           = 'jdbc:postgresql://postgrescat:5432/flink_catalog?currentSchema=paimon_catalog'
    ,'jdbc.user'                     = 'dbadmin'
    ,'jdbc.password'                 = 'dbpassword'
    ,'jdbc.driver'                   = 'org.postgresql.Driver'
    -- MinIO S3 configuration with SSL/TLS (if needed)
    ,'warehouse'                     = 's3://warehouse/paimon'      -- bucket / datastore
    ,'s3.endpoint'                   = 'http://minio:9000'          -- MinIO endpoint
    ,'s3.path-style-access'          = 'true'                       -- Required for MinIO
    -- Default table properties
    ,'table-default.file.format'     = 'parquet'
);

USE CATALOG c_paimon;

CREATE DATABASE IF NOT EXISTS c_paimon.finflow;

SHOW DATABASES;


-- Iceberg based REST Catalog stored inside PostgreSQL database using Polaris as catalog store
-------------------------------------------------------------------------------------------------------------------------
-- server: postgrescat
-- db: findept
-- schema: polaris_schema
CREATE CATALOG c_iceberg WITH (
   'type'='iceberg'
  ,'catalog-type'='rest'
  ,'uri'='http://polaris:8181/api/catalog'
  ,'warehouse'='icebergcat'
  ,'oauth2-server-uri'='http://polaris:8181/api/catalog/v1/oauth/tokens'
  ,'credential'='root:s3cr3t'
  ,'scope'='PRINCIPAL_ROLE:ALL'
  ,'s3.endpoint'='http://minio:900'
  ,'s3.access-key-id'='mnadmin'
  ,'s3.secret-access-key'='mnpassword'
  ,'s3.path-style-access'='true'
);


USE CATALOG c_iceberg;
-- Create using below
CREATE DATABASE IF NOT EXISTS c_iceberg.finflow;

-- create using api, see polaris/README.md
CREATE DATABASE IF NOT EXISTS c_iceberg.fraud;


SHOW DATABASES;

-- Paimon based REST Catalog stored inside PostgreSQL database using Polaris as catalog store
-- server: postgrescat
-- db: findept
-- schema: polaris_schema
CREATE CATALOG c_paimon WITH (
   'type'='paimon'
  ,'catalog-type'='rest'
  ,'uri'='http://polaris:8181/api/catalog'
  ,'warehouse'='paimoncat'
  ,'oauth2-server-uri'='http://polaris:8181/api/catalog/v1/oauth/tokens'
  ,'credential'='root:s3cr3t'
  ,'scope'='PRINCIPAL_ROLE:ALL'
  ,'s3.endpoint'='http://minio:900'
  ,'s3.access-key-id'='mnadmin'
  ,'s3.secret-access-key'='mnpassword'
  ,'s3.path-style-access'='true'
);

USE CATALOG c_paimon;

CREATE DATABASE IF NOT EXISTS c_paimon.finflow;

SHOW DATABASES;


-- Fluss Bits
CREATE CATALOG c_fluss WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG c_fluss;

CREATE DATABASE IF NOT EXISTS c_fluss.finflow;

SHOW DATABASES;

-- next execute 2.1

-- next execute 3.1

-- next execute 4.1