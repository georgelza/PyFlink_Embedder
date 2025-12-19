-- scripts/02_register_embed_udfs.sql

-- Load Python UDF from file
CREATE TEMPORARY FUNCTION embed_transactions AS 'txn_embed_udf.main'
LANGUAGE PYTHON USING JAR 'file:///pyflink/udfs/txn_embed_udf.py';
