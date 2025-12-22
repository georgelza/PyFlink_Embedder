
-- scripts/test_upper_udf.sql
-- Test the UDF before running the full INSERT


SELECT upper_case('hello world');

-- Test with source table (preview)
SELECT 
     _id
    ,firstname
    ,lastname
    ,upper_case(firstname) as upper_firstname
    ,upper_case(lastname)  as upper_lastname
FROM c_cdcsource.demog.accountholders
LIMIT 5;
