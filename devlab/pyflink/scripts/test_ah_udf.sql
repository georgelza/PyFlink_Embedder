
-- scripts/test_ah_udf.sql
-- Test the UDF before running the full INSERT





-- Test with source table (preview)
SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql'; 


SELECT 
     _id                
    ,nationalid         
    ,firstname          
    ,lastname           
    ,generate_ah_embedding(
         384
        ,firstname 
        ,lastname 
        ,dob
        ,gender
        ,children 
        ,address 
        ,accounts
        ,emailaddress 
        ,mobilephonenumber
    )                       AS embedding_vector
    ,CURRENT_TIMESTAMP      AS embedding_timestamp
    ,created_at
FROM c_cdcsource.demog.accountholders;

-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS embedding_timestamp;

USE CATALOG c_cdcsource;
CREATE DATABASE IF NOT EXISTS demog;  
USE demog;

-- 
SET 'execution.checkpointing.interval'  = '60s';
SET 'pipeline.name'                     = 'Emded & Persist into Paimon (finflow): accountholders';

INSERT INTO c_paimon.finflow.accountholders 
SELECT 
     _id                
    ,nationalid         
    ,firstname          
    ,lastname           
    ,dob                 
    ,gender             
    ,children           
    ,address            
    ,accounts           
    ,emailaddress       
    ,mobilephonenumber  
    ,generate_ah_embedding(
         384
        ,firstname 
        ,lastname 
        ,dob
        ,gender
        ,children 
        ,address 
        ,accounts
        ,emailaddress 
        ,mobilephonenumber
    )                       AS embedding_vector
    ,384                    AS embedding_dimensions
    ,CURRENT_TIMESTAMP      AS embedding_timestamp
    ,created_at
FROM c_cdcsource.demog.accountholders;