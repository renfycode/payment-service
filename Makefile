.DEFAULT_GOAL := help

UV ?= uv
RUN := $(UV) run
SOURCES := src tests
# Дополнительные аргументы pytest: make test PYTEST_ARGS="-k idempotency -x"
PYTEST_ARGS ?=

.PHONY: help install format lint typecheck check test test-unit test-integration ci

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies (including dev) from uv.lock
	$(UV) sync --locked

format: ## Format code and apply safe lint fixes
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

lint: ## Check style and formatting without changing files
	$(RUN) ruff check .
	$(RUN) ruff format --check .

typecheck: ## Static type checks: mypy (strict) and pyright
	$(RUN) mypy $(SOURCES)
	$(RUN) pyright $(SOURCES)

check: lint typecheck ## All static checks (lint + typecheck)

test: ## Run all tests (integration tests need Docker)
	$(RUN) pytest $(PYTEST_ARGS)

test-unit: ## Run unit tests only (fast, no Docker)
	$(RUN) pytest tests/unit $(PYTEST_ARGS)

test-integration: ## Run integration tests (Postgres, RabbitMQ, webhook receiver in Docker)
	$(RUN) pytest tests/integration $(PYTEST_ARGS)

ci: check test ## Everything CI runs: static checks and all tests
