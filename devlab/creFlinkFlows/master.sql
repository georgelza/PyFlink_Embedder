

-- Set Python environment
SET 'python.executable'                 = '/usr/bin/python3';
SET 'execution.runtime-mode'            = 'streaming';
SET 'execution.planner.type'            = 'streaming';
SET 'execution.checkpointing.interval'  = '60s';

-- Create catalogs
SOURCE '/creFlinkFlows/scripts/1.1.creCat.sql';

-- Create tables
SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql';
SOURCE '/creFlinkFlows/scripts/3.1.creTargetDemog.sql';
SOURCE '/creFlinkFlows/scripts/3.2.creTargetCmplx.sql';

