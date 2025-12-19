
-- make cat
-- Execute script 1: 
-- Create Catalogs
-- 1.1
SOURCE '/creFlinkFlows/scripts/1.1.creCat.sql';


-- make crecdc
-- Execute script 2: 
-- Create CDC Source Tables
-- This will always create the demog database as our catalog is generic_in_memory
-- 2.1
SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql';


-- make crefinflow
-- Execute script 3: 
-- Create our embedding output table
-- 3.1
SOURCE '/creFlinkFlows/scripts/3.1.creTargetDemog.sql';


-- Set common configurations
SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.planner.type'            = 'streaming';
SET 'execution.checkpointing.interval'  = '60s';
SET 'execution.planner.planner'         = 'blink';
SET 'execution.planner.result-mode'     = 'table';


-- make insfinflow
-- Execute script 4: 
-- Insert our data into output table, with Pyflink embedding routine included
-- 
-- 4.1 or This could possible all be build into the PyFlink Routine that get's deployed onto the c_cdcsource.demog table
-- This will first need to 'recreate our c_cdcsource.demog* tables as step 1 in the script before executing the 
-- Insert into () select () from ...
--
SOURCE '/creFlinkFlows/scripts/4.1.creInsertsDemog.sql';


-- Execute script 5: 
-- Create Structures as per 3.2


-- Execute script 6: 
-- Execute Insert as per 4.2 


-- Execute script 7: 
-- Execute CTAS as per 4.3