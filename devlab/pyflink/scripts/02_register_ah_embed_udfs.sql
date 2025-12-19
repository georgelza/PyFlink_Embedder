-- scripts/02_register_ah_embed_udfs.sql

-- Load Python UDF from file
CREATE TEMPORARY FUNCTION generate_ah_embedding AS '02_ah_embed_udf.generate_ah_embedding' 
LANGUAGE PYTHON USING JAR 'file:///pyflink/udfs/02_ah_embed_udf.py';

