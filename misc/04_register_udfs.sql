-- scripts/01_register_udfs.sql

-- Load Python UDF from file
CREATE TEMPORARY FUNCTION upper_case AS 'my_functions.upper_case' 
LANGUAGE PYTHON USING JAR 'file:///pyflink/udfs/my_functions.py';

CREATE TEMPORARY FUNCTION calculate_score AS 'my_functions.calculate_score'
LANGUAGE PYTHON USING JAR 'file:///pyflink/udfs/my_functions.py';
