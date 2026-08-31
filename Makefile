SHELL := /bin/bash
COMPOSE := docker compose -f docker/compose.yaml --env-file .env
PY := .venv/bin/python

.PHONY: help up down ps logs nuke venv

help: ## list these
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## start the stack
	$(COMPOSE) up -d --build
	@echo "Kafka UI  -> http://localhost:8088"
	@echo "MinIO     -> http://localhost:9001"
	@echo "Airflow   -> http://localhost:8082"
	@echo "MLflow    -> http://localhost:5001"

down: ## done for the day
	$(COMPOSE) --profile stream down

ps: ## what's running
	$(COMPOSE) ps

logs: ## tail everything
	$(COMPOSE) logs -f --tail=100

nuke: ## wipe it all and start over
	$(COMPOSE) --profile stream down -v
	rm -rf data chk logs
	@echo "everything done and dusted, new beginning now"


venv: ## build .venv
	uv venv --python 3.11
	uv pip install pyspark==3.5.1 kafka-python-ng faker numpy pandas psycopg2-binary \
	  python-dotenv pytest ruff dbt-core "dbt-spark[session]>=1.8"
