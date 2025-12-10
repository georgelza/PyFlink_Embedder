
## Table/Objects Structures

### 1.1.creCat.sql

Will create the various catalogs and databases.

 - postgres_catalog - Generic InMemory
   - demog

 - c_iceberg - Apache Polaris REST based
   - finflow

 - c_paimon - Apache Flink JDBC based
   - finflow

 - c_fluss - Apache Flunns based

### 2.1.creCdc.sql

This will create our transciant CDC based tables which will connect to our PostgreSQL datastore and expose data using the Flink CDC capabilities

 - accountholders

 - transactions 

### 3.1.creEmbTargets.sql

Create our output tables that will recieve the "vectorized/embedding vectors" enriced records.

### 3.2.creTarget.sql

???

### 4.1creInserts.sql

???


