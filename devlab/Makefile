
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- build: 		Build required Docker images (Flink with CDC)

- run:   		Start All services (Flink, PostgreSQL & Minio)
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


# S3/Minio based  => Default
master:
	@echo "-- Generated master.sql" > ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/1.1.creCat.sql >> ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/3.1.creTargetFinflow.sql >> ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/3.2.creTargetCmplx.sql >> ./creFlinkFlows/master.sql
	@echo "✅ master.sql for S3 generated"

deploy: master
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/master.sql


# File System based
master-fs:
	@echo "-- Generated master.sql" > ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/1.1.creCat-fs.sql >> ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/3.1.creTargetFinflow.sql >> ./creFlinkFlows/master.sql
	@cat ./creFlinkFlows/scripts/3.2.creTargetCmplx.sql >> ./creFlinkFlows/master.sql
	@echo "✅ master.sql for FS generated"

deploy-fs: master-fs
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/master.sql


# Single threaded/worker
ah1:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.1.creInsertsAhSingle.sql


# 4 workers -> Problems
ahm:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.1.creInsertsAhMulti.sql


#txn:
# Placeholder
# Executed by executing the txn_embed.cmd job at the Jobmanager CLI


ins_cmplx:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.3.creInsertsCmplx.sql -s /creFlinkFlows/config/sql-client-config.yaml

ctas:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.4.creCtas.sql -s /creFlinkFlows/config/sql-client-config.yaml


jm:
	docker compose exec --interactive --tty jobmanager /bin/bash