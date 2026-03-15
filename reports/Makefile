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

SRC_PATH := "./reports"

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

.PHONY: auto-format
auto-format: ## Automatically formats the code
	uv run ruff format $(SRC_PATH) ./tests

.PHONY: check
check: auto-format lint ## Runs all code checks

.PHONY: run
run: ## Runs notebook environment
	uv run marimo edit
