
## Boot strapping our environment.

From within `<Project root>/devlab/`

We can take the environment through various phases. 


- Our `devlab/creFlinkFlows/1.1.creCat.sql` script also provides the required command to create Paimon based catalog.
  
- If you want, you can deploy the Apache Flink Cluster, allowing you move data across the Flink stack and additionally the accompanying PyFlink routines that will calculate vector embedding values for the accountholders and transactions. These values will be pushed as a new record into accountholder and transactions tables (which will be stored in Apache Paimon).

  
  
### 1. MinIO/S3 Based deployment

`make run`

This will bring up our Apache Flink, JDBC based catalog with PostgreSQL for persistence and lakehouser storage on S3 based Object storage by MinIO service.

`make deploy`

This will create our `c_paimon` catalog on the MinIO/S3 `warehouse/paimon` object store.


### 2. FilesystemS3 Based deployment

`make run-fs`

This will bring up our Apache Flink, JDBC based catalog with PostgreSQL for persistence and lakehouse storage on local file system.

`make deploy-fs`

This will create our `c_paimon` catalog on the local file system into `./data/flink/paimon` as mounted into container as /data.


### 3. Deploy single worker Embedding job for our accountHolder data products

- `make ahs`


### 4. Deploy single worker Embedding job for our transactions data products

- `make txns`


### 5. Run our Shadowtraffic data generator

From within `<Project root>/shadowtraffic`

Execute run_pg1.sh