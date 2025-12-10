
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- build: Build required Docker images (Flink with CDC)

- run_base:		Start Base Polaris services (Polaris, PostgreSQL & Minio)
- run_flink:   	Start Flink and Polaris services (Flink, Polaris, PostgreSQL & Minio)
- run:   		Start All services (Flink, Fluss, Polaris, PostgreSQL & Minio)

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


run_base:
	@echo "🚀 Starting Polaris Catalog LAB configuration (Polaris, PostgreSQL & MinIO)..."
	docker compose -f docker-compose-basic.yml -p aiembed up -d \
 		postgrescat \
		minio minio-client \
		polaris polaris-bootstrap polaris-setup \
		--remove-orphans
	@echo "✅ Polaris Catalog LAB services started successfully"
.PHONY: run_base

run_flink:
	@echo "🚀 Starting Flnk + Polaris Catalog LAB configuration (Flink, Polaris, PostgreSQL & MinIO)..."
	docker compose -f docker-compose-flink.yml -p aiembed up -d \
		jobmanager taskmanager \
 		postgrescat postgrescdc \
		minio minio-client \
		polaris polaris-bootstrap polaris-setup \
		--remove-orphans
	@echo "✅ Flink & Polaris Catalog LAB services started successfully"
.PHONY: run_flink

run:
	@echo "🚀 Starting All services (Flink, Fluss, Polaris, PostgreSQL & MinIO)..."
	docker compose -f docker-compose-all.yml -p aiembed up -d \
		jobmanager taskmanager \
		coordinator tablet-server-0 tablet-server-1 tablet-server-2 \
 		postgrescat postgrescdc \
		minio minio-client \
		polaris polaris-bootstrap polaris-setup \
		--remove-orphans
	@echo "✅ All services started successfully"
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
	cd data/; rm -rf polaris


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