
-- Thinking is we will consume from PostgreSQL sources, then use a PyFlink to flatten the structure, outputting to a Fluss table.
-- then reading new Fluss table, calculate embeddings, output to another Fluss table/s. 
-- This in turn will be configured with Lakehouse tiering into Paimon table on local FS into /data directory 

-- Catalog c_cdcsource moved to 2.1.creCdc.sql
-- This is due t this being an generic in memory catalog and is required to be re-created in every session together with the 
-- CDC backed tables.

USE CATALOG default_catalog;
  
CREATE CATALOG c_cdcsource WITH 
    ('type'='generic_in_memory'); 

USE CATALOG c_cdcsource;

-- Source for PyFlink
CREATE DATABASE IF NOT EXISTS demog;            

-- Paimon based Catalog stored inside PostgreSQL database using JDBC interface
-------------------------------------------------------------------------------------------------------------------------
-- server: postgrescat
-- db:     flink_catalog
-- schema: paimon_catalog
CREATE CATALOG c_paimon WITH (
     'type'                          = 'paimon'
    ,'metastore'                     = 'jdbc'                      
    ,'catalog-key'                   = 'jdbc'
    ,'uri'                           = 'jdbc:postgresql://postgrescat:5432/flink_catalog?currentSchema=paimon_catalog'
    ,'jdbc.user'                     = 'dbadmin'
    ,'jdbc.password'                 = 'dbpassword'
    ,'jdbc.driver'                   = 'org.postgresql.Driver'
    ,'warehouse'                     = 'file:///paimon'
    ,'table-default.file.format'     = 'parquet'
);

USE CATALOG c_paimon;

-- Output from PyFlink routine, embedded tables
CREATE DATABASE IF NOT EXISTS c_paimon.finflow;       

SHOW DATABASES;


-- next execute 2.1
