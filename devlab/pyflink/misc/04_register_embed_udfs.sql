-- scripts/01b_register_embed_udfs.sql

-- Add Python dependencies
ADD JAR 'file:///path/to/python-dependencies.zip';

-- Register UDF
CREATE TEMPORARY FUNCTION embed_accountholders AS 'ah_embed_udf.main'
LANGUAGE PYTHON 
USING JAR 'file:///pyflink/udfs/ah_embed_udf.py';

-- Register UDF
CREATE TEMPORARY FUNCTION embed_transactions AS 'txn_embed_udf.main'
LANGUAGE PYTHON 
USING JAR 'file:///pyflink/udfs/txn_embed_udf.py';