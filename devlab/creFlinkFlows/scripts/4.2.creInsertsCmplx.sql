-- Transform c_paimon.finflow into complex JSON structured record.

-- See 4.1
USE CATALOG c_paimon;



USE cmplx;

-- SET 'execution.checkpointing.interval'   = '60s';
-- SET 'table.exec.sink.upsert-materialize' = 'NONE';

SET 'pipeline.name' = 'Persist into Paimon (cmplx): accountholders';

INSERT INTO accountholders 
    () AS
SELECT () 
FROM c_paimon.finflow.accountholders;


SET 'pipeline.name' = 'Persist into Paimon (cmplx): transactions';

INSERT INTO transactions 
    () AS
SELECT () 
FROM c_paimon.finflow.transactions;