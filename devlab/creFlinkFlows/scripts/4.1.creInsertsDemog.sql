-- Insert Statement with embedded call to our PyFlink Embedding routine.
-- Primary c_paimon.finflow output database

USE CATALOG c_paimon;



USE finflow;

SET 'execution.checkpointing.interval'   = '60s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'pipeline.name' = 'Persist into Paimon (finflow): accountholders';

INSERT INTO accountholders 
    () AS
SELECT () 
FROM c_cdcsource.demog.accountholders;


SET 'pipeline.name' = 'Persist into Paimon (finflow): transactions';

INSERT INTO transactions 
    () AS
SELECT () 
FROM c_cdcsource.demog.transactions;

-- See 4.2 and 4.3