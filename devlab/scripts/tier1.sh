#!/bin/bash

# See: https://fluss.apache.org/docs/maintenance/tiered-storage/lakehouse-storage/

$FLINK_HOME/bin/flink run \
    -Dpipeline.name="My Fluss Tiering Service To S3 MinIO" \
    -Dparallelism.default=3 \
    $FLINK_HOME/lib/fluss-flink-tiering-0.8.0-incubating.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore jdbc \
    --datalake.paimon.catalog-key jdbc \
    --datalake.paimon.uri jdbc:postgresql://postgrescat:5432/flink_catalog?currentSchema=paimon \
    --datalake.paimon.jdbc.user dbadmin \
    --datalake.paimon.jdbc.password dbpassword \
    --datalake.paimon.warehouse s3://warehouse/paimon \
    --datalake.paimon.s3.endpoint http://minio:9000 \
    --datalake.paimon.s3.path-style-access true \
    --datalake.paimon.table-default.file.format parquet