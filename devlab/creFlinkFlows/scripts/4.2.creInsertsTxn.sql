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


# see pyflink/txn_embed.cmd

/opt/flink/bin/flink run \
    -m jobmanager:8081 \
    -py /pyflink/udfs/txn_embed_udf.py \
    -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar \
    -j /opt/flink/lib/flink-python-1.20.1.jar \
    -j /opt/flink/lib/postgresql-42.7.6.jar     