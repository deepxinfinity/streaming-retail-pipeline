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

ps: ## whatss running
	$(COMPOSE) ps

logs: ## tail everything
	$(COMPOSE) logs -f --tail=100

nuke: ## wipe it all and start over
	$(COMPOSE) --profile stream down -v
	rm -rf data chk logs
	@echo "everything done and dusted, new beginning now"


venv: # build .venv
	uv venv --python 3.11
	uv pip install pyspark==3.5.1 kafka-python-ng faker numpy pandas psycopg2-binary \
	  python-dotenv pytest ruff dbt-core "dbt-spark[session]>=1.8"

.PHONY: seed lint test

# simulator / kafka
seed: ## master data into postgres
	$(PY) -m sim.seed

#  tests
lint: ## ruff
	.venv/bin/ruff check .

test: lint ## ruff + pytest
	.venv/bin/pytest -q

.PHONY: sample produce backfill

M ?= $(shell grep -s BACKFILL_MONTHS .env | cut -d= -f2 | cut -d' ' -f1)

sample: # print a few live messages from both topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:29092 --topic online_orders --max-messages 3 --from-beginning
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:29092 --topic store_pos --max-messages 3 --from-beginning

produce: # run both live producers 
	bash scripts/run_producers.sh

backfill: # replay M months of history through Kafka likre make backfill M=1
	BACKFILL_MONTHS=$(M) $(PY) -m sim.backfill
