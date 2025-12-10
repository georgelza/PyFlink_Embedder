.DEFAULT_GOAL := help

define HELP

Available commands:

- pullall: Pull all source images.

- buildall: Build all images.

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


build:
	@echo "🔨 Building all Docker images..."
#	@cd postgres && make pull && make build
#	@cd flink && make pull && make build
	@cd flink && make build
	@echo "✅ All images built successfully"
.PHONY: build


pull: flink fluss shadowtraffic datastores
	@echo "✅ All source images pulled successfully"
.PHONY: pull


flink:
	docker pull flink/flink:1.20.2-scala_2.12-java17

fluss:
	docker pull apache/fluss:0.8.0-incubating
	docker pull zookeeper:3.9.2

shadowtraffic:
	docker pull shadowtraffic/shadowtraffic:latest

datastores:
	docker pull postgres:12
	docker pull quay.io/minio/minio:RELEASE.2024-12-18T13-15-44Z

catalog:
	docker pull apache/polaris:latest
	
# Using above image as it still includes the full console functionality 
#	docker pull minio/minio:latest
