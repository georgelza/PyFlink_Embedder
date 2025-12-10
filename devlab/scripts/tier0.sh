#!/bin/bash

# See: https://fluss.apache.org/docs/maintenance/tiered-storage/lakehouse-storage/

$FLINK_HOME/bin/flink run \
    -Dpipeline.name="My Fluss Tiering Service To Disk" \
    -Dparallelism.default=3 \
    $FLINK_HOME/lib/fluss-flink-tiering-0.8.0-incubating.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon