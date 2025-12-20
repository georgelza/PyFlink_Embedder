-- This is purely a Placeholder for the Transactions table.
-- The insert will be accomplished by the Python based UDF function, see devlab/pyflink/txn_embed._udf.py and txn_embed.cmd
-- => c_paimon.finflow.transactions, sourced from c_cdcsource.demog.transactions
USE CATALOG c_paimon;



USE finflow;

-- Environment is configured using the -s /creFlinkFlows/config/sql-client-config.yaml
-- SET 'execution.checkpointing.interval'   = '60s';
-- SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- Recreate the CDC tables inside this session.

SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql'; 

SET 'pipeline.name' = 'Emded & Persist into Paimon (finflow): transactions';

