SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

SRC_PATH := "./data_pipelines"
AIRFLOW_VERSION=3.1.3

.PHONY: help
help: ## Shows help for targets with help text
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

.PHONY: install
install: ## Installs all dependencies
	uv sync

.PHONY: requirements.txt
requirements.txt: ## Creates requirements.txt file
	uv export --no-dev --no-editable --no-emit-project --format requirements.txt > requirements.txt

.PHONY: lint
lint: ## Checks code formatting and style
	uv run ruff check $(SRC_PATH) ./tests

.PHONY: type-check
type-check: ## Checks types
	uv run mypy --ignore-missing-imports --strict --allow-subclassing-any \
		$(SRC_PATH)

.PHONY: auto-format
auto-format: ## Automatically formats the code
	uv run ruff format $(SRC_PATH) ./tests

.PHONY: unit-test
unit-test: ## Runs all unit tests
	uv run pytest -v -s ./tests/unit

.PHONY: integration-test
integration-test: ## Runs all integration tests
	uv run pytest -v -s ./tests/integration

.PHONY: test
test: unit-test integration-test ## Runs all tests

.PHONY: check
check: auto-format lint type-check test ## Runs all code checks and tests

.PHONY: run
run: ## Starts airflow locally
	AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__DAGS_FOLDER=$(shell pwd)/$(SRC_PATH) AIRFLOW_HOME=$(shell pwd)/airflow uv run airflow standalone

.PHONY: clean
clean: ## Cleans up local state of airflow and in the datalake
	rm -rf airflow/*
		touch airflow/.git-keep
		rm -rf ../datalake/bronze/*.db
		rm -rf ../datalake/silver/*.db
		rm -rf ../datalake/gold/*.db
