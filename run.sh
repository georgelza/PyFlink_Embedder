#!/bin/bash
. ./.pws   


# 1. Flink - see .pws

# 2. Fluss

# 3. PostgreSQL
# CAT Source
export POSTGRES_CAT_HOST=localhost
export POSTGRES_CAT_PORT=5432
export POSTGRES_CAT_USER=dbadmin
# export POSTGRES_CAT_PASSWORD=
export POSTGRES_CAT_DB=catalog     

# CDC Source
export POSTGRES_CDC_HOST=localhost
export POSTGRES_CDC_PORT=5432
export POSTGRES_CDC_USER=dbadmin
# export POSTGRES_CDC_PASSWORD=
export POSTGRES_CDC_DB=demog     


# 4. Minio
export MINIO_ROOT_USER=mnadmin
#export MINIO_ROOT_PASSWORD= 
export MINIO_ALIAS=minio
export MINIO_ENDPOINT=http://minio:9000
export MINIO_BUCKET=warehouse

# 5.