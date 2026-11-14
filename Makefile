SHELL := /usr/bin/env bash

.PHONY: up init produce produce-once consume run-pipeline dashboard test down validate

up:
	docker compose up -d postgres minio minio-init kafka airflow-init airflow-webserver airflow-scheduler dashboard

init:
	python scripts/seed_reference_data.py
	docker compose exec -T kafka bash /opt/streamcommerce/scripts/init_kafka_topics.sh

produce:
	python -m streaming.producers.event_simulator --source customer_events --event-rate 5 --duration 0 --bad-data-percentage 8

produce-once:
	python -m streaming.producers.event_simulator --source orders --event-rate 10 --duration 15 --bad-data-percentage 10

consume:
	python -m streaming.consumers.s3_landing_consumer --landing-root data/landing

run-pipeline:
	bash scripts/run_local_pipeline.sh

dashboard:
	python dashboard/app.py

test:
	pytest -q

validate:
	bash scripts/validate_project.sh

down:
	docker compose down --remove-orphans
