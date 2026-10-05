.DEFAULT_GOAL := help

UV ?= uv
RUN := $(UV) run
SOURCES := src tests scripts
COMPOSE := docker compose
# Дополнительные аргументы pytest: make test PYTEST_ARGS="-k idempotency -x"
PYTEST_ARGS ?=

.PHONY: help install format lint typecheck check test test-unit test-integration ci \
	local-start local-smoke local-logs local-stop local-clean

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

local-start: ## Build and start the full stack in Docker (postgres, rabbitmq, api, consumer)
	$(COMPOSE) up -d --build --wait
	@echo ""
	@echo "  API          http://localhost:8000  (X-API-Key: dev-api-key)"
	@echo "  Swagger UI   http://localhost:8000/docs"
	@echo "  RabbitMQ UI  http://localhost:15672  (payments / payments)"
	@echo ""
	@echo "  Check it end-to-end: make local-smoke    Logs: make local-logs"

local-smoke: ## Start the stack with a webhook receiver and run an end-to-end smoke test
	$(COMPOSE) --profile smoke up -d --build --wait
	$(RUN) python scripts/smoke.py

local-logs: ## Follow api and consumer logs
	$(COMPOSE) logs -f api consumer

local-stop: ## Stop the stack, keep data (database, queues)
	$(COMPOSE) --profile smoke down

local-clean: ## Stop the stack and delete all data volumes
	$(COMPOSE) --profile smoke down -v
