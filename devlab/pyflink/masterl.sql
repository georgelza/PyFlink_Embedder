-- master.sql

-- Set Python environment
SET 'python.executable' = '/usr/bin/python3';
SET 'python.client.executable' = '/usr/bin/python3';

-- Create tables
SOURCE '/creFlinkFlows/scripts/2.1.creCdcDemog.sql';
SOURCE '/creFlinkFlows/scripts/3.1.creTargetDemog.sql';
SOURCE '/creFlinkFlows/scripts/3.2.creCmplxTarget.sql';

-- Register UDFs
SOURCE '/pyflink/scripts/02_register_ah_embed_udfs.sql';
SOURCE '/pyflink/scripts/03_register_txn_embed_udfs.sql';
SOURCE '/pyflink/scripts/04_register_udfs.sql';

-- Use UDFs in queries
-- SOURCE '/pyflink/scripts/????.sql';

