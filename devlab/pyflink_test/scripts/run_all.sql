-- scripts/run_all.sql
-- Complete UDF registration and execution script
-- Flink Version: 1.20.x

-- ============================================================================
-- Step 1: Add below to devlab/conf/config.yaml
-- This tells Flink how to work with Python and where the functions will be
-- ============================================================================
python: 
  client:
    executable: python3
  executable: python3
  files: /pyflink/udfs


-- ============================================================================
-- Step 2: Register the UDF
-- ============================================================================
CREATE TEMPORARY FUNCTION upper_case AS 'upper_udf.uppercase_str' LANGUAGE PYTHON;

-- DROP TEMPORARY FUNCTION upper_case IF EXIST;
--

-- ============================================================================
-- Step 3: Verify UDF registration (optional)
-- ============================================================================
-- SHOW FUNCTIONS;

-- ============================================================================
-- Step 4: Test the UDF (optional but recommended)
-- ============================================================================
-- Quick test with literal value
SELECT upper_case('hello world');

-- Test with source table data (preview first 5 rows)
SELECT 
     id
    ,firstname,
    ,lastname,
    ,upper_case(firstname) as upper_firstname
    ,upper_case(lastname)  as upper_lastname
FROM c_cdcsource.demog.accountholders
LIMIT 5;


-- ============================================================================
-- Step 5: Execute the INSERT query
-- ============================================================================
INSERT INTO sink_table (id, firstname, lastname, upper_firstname, upper_lastname)
SELECT 
     id
    ,firstname,
    ,lastname,
    ,upper_case(firstname) as upper_firstname
    ,upper_case(lastname)  as upper_lastname
FROM c_cdcsource.demog.accountholders
LIMIT 5;