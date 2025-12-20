
-- make cat
-- Execute script 1: 
-- Create Catalogs and databases

SOURCE '/creFlinkFlows/scripts/1.1.creCat.sql';


-- make crecdc
-- Execute script 2: 
-- Create CDC Source Tables
-- This will always create the demog database as our catalog is generic_in_memory

SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql';


-- make crefinflow
-- Execute script 3: 
-- Create our flat structured embedding output table

SOURCE '/creFlinkFlows/scripts/3.1.creTargetFinflow.sql';


-- make crecmplx
-- Execute script 4: 
-- Create our complex structured target output tables

SOURCE '/creFlinkFlows/scripts/3.2.creTargetCmplx.sql';


-- Set common configurations
-- The below values are also executed/injected using the -s <creFlinkFlows/config/*.yaml> input param file to the /opt/flink/bin/sql-client.sh 
SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.planner.type'            = 'streaming';
SET 'execution.checkpointing.interval'  = '60s';
SET 'execution.planner.planner'         = 'blink';
SET 'execution.planner.result-mode'     = 'table';


-- make ins_finflow
-- Execute script 5: 
-- Insert our data into output table, using insert statement with inline Pyflink embedding routine called.

SOURCE '/creFlinkFlows/scripts/4.1.creInsertsAh.sql';


-- make ins_ah
-- Execute script 6: 
-- Insert our data into output table, using Pyflink job, with Pyflink embedding routine included

SOURCE '/creFlinkFlows/scripts/4.2.creInsertsTxn.sql';


-- Execute script 7: 
-- Insert records into our two Complex structures tables (c_paimon.cmplx.*), source from 4.1 and 4.2 

SOURCE '/creFlinkFlows/scripts/4.3.creInsertsCmplx.sql';


-- Execute script 8: 
-- Execute CTAS to create two tables in c_paimon.ctas using CTAS pattern, to potentially form part of an additional outbound data flow 

SOURCE '/creFlinkFlows/scripts/4.4.creCtas.sql';
