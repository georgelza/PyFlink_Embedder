-- scripts/register_txn_embed_udfs.sql
-- Register Python UDF for use in Flink SQL Client
-- Flink Version: 1.20.x

USE CATALOG c_cdcsource;
CREATE DATABASE IF NOT EXISTS demog;  
USE demog;

SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-python-1.20.1.jar;file:///opt/flink/lib/postgresql-42.7.6.jar;file:///opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar';

-- Load Python UDF from file
DROP FUNCTION IF EXISTS generate_txn_embedding;

-- Register the UDF
-- Syntax: CREATE FUNCTION <function_name> AS '<module_name>.<function_name>' LANGUAGE PYTHON;
CREATE FUNCTION generate_txn_embedding 
    AS 'txn_embed_udf.generate_txn_embedding' 
    LANGUAGE PYTHON;

