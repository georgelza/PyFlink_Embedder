-- scripts/register_ah_embed_udfs.sql
-- Register Python UDF for use in Flink SQL Client
-- Flink Version: 1.20.x

USE CATALOG c_cdcsource;
CREATE DATABASE IF NOT EXISTS demog;  
USE demog;

-- Load Python UDF from file
DROP FUNCTION IF EXISTS generate_ah_embedding;

-- Register the UDF
-- Syntax: CREATE FUNCTION <function_name> AS '<module_name>.<function_name>' LANGUAGE PYTHON;
CREATE FUNCTION generate_ah_embedding 
    AS 'ah_embed_udf.generate_ah_embedding' 
    LANGUAGE PYTHON;