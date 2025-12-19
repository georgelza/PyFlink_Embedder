-- scripts/02_register_txn_embed_udfs.sql

-- Load Python UDF from file
CREATE TEMPORARY FUNCTION generate_txn_embedding AS 'txn_embed_udf.generate_txn_embedding'
LANGUAGE PYTHON USING JAR 'file:///pyflink/udfs/txn_embed_udf.py';
