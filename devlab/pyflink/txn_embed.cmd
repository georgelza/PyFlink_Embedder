 
# in `devlab` directory execute:

make jm

# now copy and paste the below into the prompt of the jobmanager.

******** Calculate embedding vectors for our transactions
******** Source from c_cdcsource.demog.transactions
******** Output to c_paimon.finflow.transactions

/opt/flink/bin/flink run \
    -m jobmanager:8081 \
    -py /pyflink/udfs/txn_embed_udf.py \
    -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar \
    -j /opt/flink/lib/flink-python-1.20.1.jar
