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
    ,'scan.incremental.snapshot.chunk.size' = '4096'    -- Explicitly set chunk size
    ,'scan.snapshot.fetch.size'             = '512'     -- Add fetch size
    ,'connect.timeout'                      = '30s'     -- Add connection timeout
);

DROP FUNCTION IF EXISTS generate_ah_embedding;

-- Register the UDF
-- Syntax: CREATE FUNCTION <function_name> AS '<module_name>.<function_name>' LANGUAGE PYTHON;
CREATE FUNCTION generate_ah_embedding 
    AS 'ah_embed_udf.generate_ah_embedding' 
    LANGUAGE PYTHON;


-- Set common configurations
-- The below values are also executed/injected using the -s <creFlinkFlows/config/*.yaml> input param file to the /opt/flink/bin/sql-client.sh 
-- Set common configurations
-- SET 'execution.runtime-mode'            = 'streaming';
-- SET 'execution.planner.type'            = 'streaming';

SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'parallelism.default'               = '4';
SET 'python.fn-execution.bundle.size'   = '1000';
SET 'python.fn-execution.bundle.time'   = '1000';

-- Increase the timeout for the Python worker to "check-in" 
-- this helps if the model load is making the CPU sluggish
SET 'python.fn-execution.framework.bundle.size' = '100';
SET 'python.fn-execution.buffer.limit'          = '32mb';

-- Ensure the TaskManager gives the Python process enough time to start
-- 2 minutes
SET 'python.fn-execution.harness.wait-timeout' = '120000'; 

SET 'pipeline.name'                     = 'Emded & Persist into Paimon (finflow): accountholders';

-- Forcing our process to run parallel across the 4 workers.
-- Otherwise you end with 4 buckets, but with allot of 0 size parquet files.
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
FROM (
    SELECT *, 
      -- Use a simple modulo on ID to create a distribution key
      -- Since _id is BIGINT, this is a standard numeric operation
      CAST((_id % 4) AS INT) as shuffle_key
    FROM c_cdcsource.demog.accountholders
)
-- Grouping by the shuffle key and the primary key forces Flink 
-- to redistribute the stream before the UDF is called
GROUP BY shuffle_key, _id, nationalid, firstname, lastname, dob, gender, children, address, accounts, emailaddress, mobilephonenumber, created_at;
