
-- scripts/test_ah_udf.sql
-- Test the UDF before running the full INSERT





-- Any time we want to use the demog objets we need to recreate them
-- as they're stored in Generic in memory.
SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql'; 

-- CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS embedding_timestamp;
-- As we're creating the function on the cdc table in c_cdcsource we need to recreate it, over and over... :(
SOURCE '/pyflink/scripts/register_ah_embed_udf.sql'; 

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

