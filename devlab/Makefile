
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


run:
	@echo "🚀 Starting Flnk + JDBC based Catalog configuration (Flink, PostgreSQL & MinIO)..."
	docker compose -f docker-compose.yml -p aiembed up -d \
		jobmanager taskmanager \
 		postgrescat postgrescdc \
		minio minio-client \
		--remove-orphans
	@echo "✅ Flink with JDBC based Catalog and supporting services started successfully"
.PHONY: run

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


cat:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/1.1.creCat.sql

crecdc:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/2.1.creCdcDemog.sql

crefinflow:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/3.1.creTargetDemog.sql

crecmplx:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/3.2.creCmplxTarget.sql

# Register our UDF's before continuing.
register_ah_udf:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /pyflink/scripts/register_ah_embed_udfs.sql

register_txn_udf:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /pyflink/scripts/register_txn_embed_udfs.sql

# back to the scheduled programming
ins_ah:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.1.creInsertsAh.sql -s /creFlinkFlows/config/sql-client-config.yaml

#ins_txn:
# Placeholder
# Executed by executing the txn_embed.cmd job at the Jobmanager CLI

ins_cmplx:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.3.creInsertsCmplx.sql -s /creFlinkFlows/config/sql-client-config.yaml

ctas:
	docker compose exec --interactive --tty jobmanager /opt/flink/bin/sql-client.sh -f /creFlinkFlows/scripts/4.4.creCtas.sql -s /creFlinkFlows/config/sql-client-config.yaml


jm:
	docker compose exec --interactive --tty jobmanager /bin/bash