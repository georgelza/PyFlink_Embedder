.DEFAULT_GOAL := help
include ../.env

IMAGE_NAME=apacheflink-base-1.20.2-scala_2.12-java17
VERSION=1.0.2

define HELP

Available commands:

- build: Build this project

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

build: 
	docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/${IMAGE_NAME}:${VERSION} --file Dockerfile . --no-cache

pull_base:
	# docker pull arm64v8/flink:1.20.2-scala_2.12-java17
	cd stage; wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.9/bundle-2.31.9.jar \
    cd stage; wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.6/postgresql-42.7.6.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.5.0/flink-sql-connector-postgres-cdc-3.5.0.jar
	
pull: pull_base
# 1.20.2  
	cd stage: wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-json/1.20.2/flink-sql-json-1.20.2.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.20.2/flink-json-1.20.2.jar \
	cd stage; wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.20.2/flink-sql-parquet-1.20.2.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/1.3.1/paimon-s3-1.3.1.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.20/1.2.0/paimon-flink-1.20-1.2.0.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.8.0-incubating/fluss-flink-1.20-0.8.0-incubating.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/0.8.0-incubating/fluss-flink-tiering-0.8.0-incubating.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-s3/0.8.0-incubating/fluss-fs-s3-0.8.0-incubating.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/0.8.0-incubating/fluss-lake-paimon-0.8.0-incubating.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.9.1/iceberg-flink-runtime-1.20-1.9.1.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-1.20/1.9.1/iceberg-flink-1.20-1.9.1.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/0.8.0-incubating/fluss-lake-iceberg-0.8.0-incubating.jar \
	cd stage; wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar


# Lance, dont know yet...
# 	cd stage; wget https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-lance/0.8.0-incubating/fluss-lake-lance-0.8.0-incubating.jar 

# See 
# https://fluss.apache.org/downloads/
# https://fluss.apache.org/docs/maintenance/tiered-storage/lakehouse-storage/#enable-lakehouse-storage