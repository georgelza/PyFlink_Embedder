
## Catalogs, Databases and Table/Objects Structures


### 1.1.creCat.sql

Will create the various catalogs and databases.

- c_cdcsource - Generic In Memory based catalog
  
  - demog

- c_paimon - Apache Flink JDBC based catalog
  
  - finflow


### 2.1.creCdcDemog.sql

This will create our transciant CDC based tables which will connect to our PostgreSQL datastore and expose data using the Flink CDC capabilities
This script will be used/called by other scripts, this is required as the catalog/database is only visible in the current session.

Catalog: c_cdcsource.demog

- accountholders

- transactions 


### 3.1.creTargetsFinflow.sql

Create our output tables that will recieve the "vectorized/embedding" records, sourced from 4.1 & 4.2

Catalog: c_paimon.finflow

- accountholders

- transactions 


### 4.1.creInsertsAhSingle.sql

Run the Insert statement with the inline UDF call to calculate the embedding values

Catalog: c_paimon.finflow

- accountholders


### 4.2.creInsertsTxnSingle.sql

Run the Insert statement with the inline UDF call to calculate the embedding values

Catalog: c_paimon.finflow

- transactions 



