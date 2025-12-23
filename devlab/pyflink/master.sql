-- master.sql

-- Set Python environment
SET 'python.executable' = '/usr/bin/python3';
SET 'python.client.executable' = '/usr/bin/python3';

-- Create catalogs
'/creFlinkFlows/scripts/1.1.creCat.sql';

-- Create tables
'/creFlinkFlows/scripts/2.1.creCdcDemog.sql';
'/creFlinkFlows/scripts/3.1.creTargetDemog.sql';
'/creFlinkFlows/scripts/3.2.creCmplxTarget.sql';

-- Register UDFs
'/pyflink/scripts/register_ah_embed_udfs.sql';

'/pyflink/scripts/register_txn_embed_udfs.sql';


