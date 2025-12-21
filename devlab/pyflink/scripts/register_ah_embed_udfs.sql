-- scripts/register_ah_embed_udfs.sql

-- Load Python UDF from file
CREATE TEMPORARY FUNCTION generate_ah_embedding AS 'ah_embed_udf.generate_embedding' 
LANGUAGE PYTHON USING FILE 'file:///pyflink/udfs/ah_embed_udf.py';
    