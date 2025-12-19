-- Insert Statement with embedded call to our PyFlink Embedding routine.
-- Primary c_paimon.finflow output database

USE CATALOG c_paimon;



USE finflow;

SET 'execution.checkpointing.interval'   = '60s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- Recreate the CDC tables inside this session.

SOURCE '/creFlinkFlows/2.1.creCdcDemog.sql'; 

SET 'pipeline.name' = 'Persist into Paimon (finflow): accountholders';

INSERT INTO accountholders 
    (
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
    ,embedding_vector
    ,embedding_dimensions
) AS
SELECT (
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
    ,embed_accountholders(nationalid, firstname, lastname, dob, gender, children, address, accounts, emailaddress, mobilephonenumber)             
    ,375
) 
FROM c_cdcsource.demog.accountholders;


-- SET 'pipeline.name' = 'Persist into Paimon (finflow): transactions';

-- INSERT INTO transactions 
--     () AS
-- SELECT () 
-- FROM c_cdcsource.demog.transactions;

-- See 4.2 and 4.3