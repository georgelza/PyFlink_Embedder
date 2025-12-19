-- scripts/01_register_embed_udfs.sql

-- Load Python UDF from file
CREATE TEMPORARY FUNCTION generate_ah_embedding AS 'ah_embed_udf.main' 
LANGUAGE PYTHON USING JAR 'file:///pyflink/udfs/ah_embed_udf.py';

