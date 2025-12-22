-- scripts/3.2.creTargetCmplx.sql
-- Insert Statement with inline call to our generate_ah_embedding Python based UDF routine.
--  => c_paimon.finflow.accountholders, sourced from c_cdcsource.demog.accountholders

USE CATALOG c_cdcsource;
CREATE DATABASE IF NOT EXISTS demog;  
USE demog;

-- Recreate the CDC tables inside this session.

CREATE OR REPLACE TABLE accountholders (
     _id                BIGINT                  NOT NULL
    ,nationalid         VARCHAR(16)             NOT NULL
    ,firstname          VARCHAR(100)
    ,lastname           VARCHAR(100)
    ,dob                VARCHAR(10) 
    ,gender             VARCHAR(10)
    ,children           INT
    ,address            STRING
    ,accounts           STRING
    ,emailaddress       VARCHAR(100)
    ,mobilephonenumber  VARCHAR(20)
    ,created_at         TIMESTAMP_LTZ(3)
    ,WATERMARK          FOR created_at AS created_at - INTERVAL '15' SECOND
    ,PRIMARY KEY (_id) NOT ENFORCED
) WITH (
     'connector'                           = 'postgres-cdc'
    ,'hostname'                            = 'postgrescdc'
    ,'port'                                = '5432'
    ,'username'                            = 'dbadmin'
    ,'password'                            = 'dbpassword'
    ,'database-name'                       = 'demog'
    ,'schema-name'                         = 'public'
    ,'table-name'                          = 'accountholders'
    ,'slot.name'                           = 'accountholders_pyflink'           -- Can't include capital letters
    ,'scan.incremental.snapshot.enabled'   = 'true'               
    ,'scan.startup.mode'                   = 'initial'            
    ,'decoding.plugin.name'                = 'pgoutput'
);

DROP FUNCTION IF EXISTS generate_ah_embedding;

-- Register the UDF
-- Syntax: CREATE FUNCTION <function_name> AS '<module_name>.<function_name>' LANGUAGE PYTHON;
CREATE FUNCTION generate_ah_embedding 
    AS 'ah_embed_udf.generate_ah_embedding' 
    LANGUAGE PYTHON;


-- Set common configurations
-- The below values are also executed/injected using the -s <creFlinkFlows/config/*.yaml> input param file to the /opt/flink/bin/sql-client.sh 
SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.planner.type'            = 'streaming';
SET 'execution.planner.planner'         = 'blink';
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


-- See 4.3 and 4.4