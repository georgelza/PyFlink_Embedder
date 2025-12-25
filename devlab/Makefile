
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- build: 		Build required Docker images (Flink with CDC)

- run:   		Start All services (Flink, PostgreSQL & Minio/S3)
- deploy:		Deploy all structures required for our MinIO/S3 based storage/JDBC Catalog

- run-fs:  		Start All services (Flink, PostgreSQL )
- deploy-fs:	Deploy all structures required for our Filesystem based storage/JDBC Catalog

- ahs			Deploy insert statement for accountHolder data feed.
- txns			Deploy insert statement for transaction data feed.

- stop:  		Stop the project
- start: 		Restart a stopped project
- down:  		Tear down the project, clean directories

- ps:    		how all running containers
- logs:  		Show/tail logs
- logsf: 		Stream logs
- watch: 		Watch logs
- fsql			Open Flink-Sql console
- jm			Open Flink Jobmanager console

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


#	docker rmi $(docker images -q --filter "dangling=true")

build:
#	@cd ../infrastructure && make pull && make build
	@cd ../infrastructure && make build
.PHONY: build


# S3/Minio based  => Default
run:
	@echo "🚀 Starting Flnk + JDBC based Catalog configuration (Flink, PostgreSQL & MinIO)..."
	docker compose -f docker-compose.yml -p aiembed up -d \
		jobmanager taskmanager \
 		postgrescat postgrescdc \
		minio minio-client \
		--remove-orphans
	@echo "✅ Flink with JDBC based Catalog and supporting services started successfully"
.PHONY: run


# File System based
run-fs:
	@echo "🚀 Starting Flnk + JDBC based Catalog configuration (Flink, PostgreSQL)..."
	docker compose -f docker-compose-fs.yml -p aiembed up -d \
		jobmanager taskmanager \
 		postgrescat postgrescdc \
		--remove-orphans
	@echo "✅ Flink with JDBC based Catalog and supporting services started successfully"
.PHONY: run-fs


stop:
	docker compose stop

start:
	docker compose start

down:	
	docker compose down 
	cd data/; rm -rf flink
	cd data/; rm -rf postgrescdc
	cd data/; rm -rf postgrescat
	cd data/; rm -rf minio

ps:
	docker compose ps

logs:
	docker compose logs

logsf:
	docker compose logs -f

watch:
	watch docker compose ps

fsql:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh

jm:
	docker compose exec --interactive --tty jobmanager /bin/bash


# S3/Minio based  => Default
master:
	@echo "-- Generated MinIO/S3 master.sql" > ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/1.1.creCat.sql >> ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/3.1.creTargetFinflow.sql >> ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/3.2.creTargetCmplx.sql >> ./creFlinkFlows/master.sql
	@echo "✅ master.sql for MinIO/S3 generated"

# File System based
master-fs:
	@echo "-- Generated fs master.sql" > ./creFlinkFlows/master-fs.sql
	@cat ./creFlinkFlows/scripts/1.1.creCat-fs.sql >> ./creFlinkFlows/master-fs.sql
	@cat ./creFlinkFlows/scripts/3.1.creTargetFinflow.sql >> ./creFlinkFlows/master-fs.sql
	@cat ./creFlinkFlows/scripts/3.2.creTargetCmplx.sql >> ./creFlinkFlows/master-fs.sql
	@echo "✅ master-fs.sql for FS generated"


deploy: master
	@echo "Deploying Paimon based Catalog with MinIO/S3 Storage..."
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/master.sql

deploy-fs: master-fs
	@echo "Deploying Paimon based Catalog with Filesystem..."
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/master-fs.sql


# Single worker/single thread
ahs:
	@echo "Submitting Account Holders Embedding Job..."
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.1.creInsertsAhSingle.sql

txns:
	@echo "Submitting Transaction Embedding Job..."
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.2.creInsertsTxnSingle.sql
