-- scripts/register_udfs.sql
-- Register Python UDF for use in Flink SQL Client
-- Flink Version: 1.20.x

DROP TEMPORARY FUNCTION IF EXISTS upper_case;

-- Register the UDF
-- Syntax: CREATE FUNCTION <function_name> AS '<module_name>.<function_name>' LANGUAGE PYTHON;
CREATE TEMPORARY FUNCTION upper_case AS 'upper_udf.uppercase_str' LANGUAGE PYTHON;