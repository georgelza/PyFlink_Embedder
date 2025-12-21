 
# in devlab directory execute:

make jm

# now copy and paste the below into the prompt of the jobmanager.

******** Calculate embedding vectors for our accountHolders
******** Source from c_cdcsource.demog.accountHolders
******** Output to c_paimon.finflow.accountHolders

/opt/flink/bin/flink run \
    -m jobmanager:8081 \
    -py /pyflink/udfs/ah_embed_udf.py
