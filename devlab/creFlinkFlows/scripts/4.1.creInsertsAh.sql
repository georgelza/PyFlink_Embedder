-- Insert Statement with inline call to our generate_ah_embedding Python based UDF routine.
-- => c_paimon.finflow.accountholders, sourced from c_cdcsource.demog.accountholders

USE CATALOG c_paimon;



USE finflow;

-- Environment is configured using the -s /creFlinkFlows/config/sql-client-config.yaml
-- SET 'execution.checkpointing.interval'   = '60s';
-- SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- Recreate the CDC tables inside this session.

SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql'; 

SET 'pipeline.name' = 'Emded & Persist into Paimon (finflow): accountholders';

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

-- SET 'pipeline.name' = 'Emded & Persist into Paimon (finflow): transactions';
-- See 4.2

-- INSERT INTO transactions 
--     () AS
-- SELECT () 
-- FROM c_cdcsource.demog.transactions;

-- See 4.3 and 4.4