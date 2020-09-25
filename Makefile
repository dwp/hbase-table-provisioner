SHELL:=bash

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	@make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .githooks
	@git submodule update --init .githooks ; \
	git config core.hooksPath .githooks \

local-build: ## Build with gradle
	gradle :unit build -x test

local-dist: ## Assemble distribution files in build/dist with gradle
	gradle assembleDist

local-test: ## Run the unit tests with gradle
	gradle --rerun-tasks unit

local-all: local-build local-test local-dist ## Build and test with gradle

integration-test: ## Run the integration tests in a Docker container
	echo "WIP"

integration-test-equality: ## Run the integration tests in a Docker container
	echo "WIP"

integration-load-test: ## Run the integration load tests in a Docker container
	echo "WIP"

.PHONY: integration-all ## Build and Run all the tests in containers from a clean start
integration-all:
	echo "WIP"

hbase-shell: ## Open an HBase shell onto the running HBase container
	docker-compose run --rm hbase shell

hbase-up: ## Bring up and provision mysql
	docker-compose -f docker-compose.yaml up -d hbase
	@{ \
		echo Waiting for hbase.; \
		while ! docker logs hbase 2>&1 | grep "Master has completed initialization" ; do \
			sleep 2; \
			echo Waiting for hbase.; \
		done; \
		sleep 5; \
		echo ...hbase ready.; \
	}

services: hbase-up ## Bring up supporting services in docker

up: services ## Bring up Reconciliation in Docker with supporting services
	docker-compose -f docker-compose.yaml up --build -d hbase_table_provisioner

restart: ## Restart HTP and all supporting services
	docker-compose restart

down: ## Bring down the HTP Docker container and support services
	docker-compose down

destroy: down ## Bring down the HTP Docker container and services then delete all volumes
	docker network prune -f
	docker volume prune -f

