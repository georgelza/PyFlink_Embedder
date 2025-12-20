-- Output embedded data flat structure into c_paimon.ctas as a select from the c_paimon.finflow
-- Might be used to push data to another output destination
-- See 4.1
USE CATALOG c_paimon;



USE ctas;

-- Environment is configured using the -s /creFlinkFlows/config/sql-client-config.yaml
-- SET 'execution.checkpointing.interval'   = '60s';
-- SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'pipeline.name' = 'Persist into Paimon (ctas): accountholders';

CREATE OR REPLACE TABLE accountholders WITH (
     'file.format'                       = 'parquet'
    ,'compaction.min.file-num'           = '2'
    ,'compaction.early-max.file-num'     = '50'
    ,'snapshot.time-retained'            = '1h'
    ,'snapshot.num-retained.min'         = '5'
    ,'snapshot.num-retained.max'         = '20'
    ,'table.exec.sink.upsert-materialize'= 'NONE'
) AS 
SELECT * FROM c_paimon.finflow.accountholders;


SET 'pipeline.name' = 'Persist into Paimon (ctas): transactions';

CREATE OR REPLACE TABLE transactions WITH (
     'file.format'                       = 'parquet'
    ,'compaction.min.file-num'           = '2'
    ,'compaction.early-max.file-num'     = '50'
    ,'snapshot.time-retained'            = '1h'
    ,'snapshot.num-retained.min'         = '5'
    ,'snapshot.num-retained.max'         = '20'
    ,'table.exec.sink.upsert-materialize'= 'NONE'
) AS 
SELECT * FROM c_paimon.finflow.transactions;


