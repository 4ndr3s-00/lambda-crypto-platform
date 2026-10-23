# ==============================================================================
# Makefile para la Plataforma Lambda (Datos Financieros y Criptomonedas)
# ==============================================================================

COMPOSE_FILE = infra/docker-compose.yml
COMPOSE_SCALE_FILE = infra/docker-compose.scale.yml
ENV_FILE = .env

# NÃºmero de workers Spark del clÃºster. Cada worker aporta
# SPARK_WORKER_CORES cores y SPARK_WORKER_MEMORY de memoria.
WORKERS ?= 3

.PHONY: help env up down restart ps logs logs-api logs-kafka logs-spark logs-airflow rag-index test clean scale-spark scale-down catchup demo

help:
	@echo "Comandos disponibles:"
	@echo "  make env           Copia .env.example a .env si no existe"
	@echo "  make up            Levanta la infraestructura base (Kafka, MinIO, Postgres, Spark, API) y pone al dia el historico (make catchup)"
	@echo "  make catchup       Trae de Binance solo los dias de historico que falten desde la ultima vez registrada"
	@echo "  make demo          Fuerza y espera una corrida visible de Batch + reconciliacion para la presentacion"
	@echo "  make up-metadata   Levanta OpenMetadata y OpenSearch (perfil metadata - US-25)"
	@echo "  make up-all        Levanta todo el stack incluyendo LLM local (perfil llm)"
	@echo "  make down          Detiene todos los contenedores"
	@echo "  make restart       Reinicia todos los servicios"
	@echo "  make ps            Muestra el estado de los contenedores"
	@echo "  make logs          Muestra logs combinados en tiempo real"
	@echo "  make scale-spark   Escala el clÃºster Spark a WORKERS workers (por defecto 3)"
	@echo "  make clean         Detiene y elimina volÃºmenes y redes creadas"

env:
	@if [ ! -f $(ENV_FILE) ]; then \
		cp .env.example $(ENV_FILE); \
		echo "Archivo $(ENV_FILE) generado desde .env.example"; \
	else \
		echo "Archivo $(ENV_FILE) ya existe."; \
	fi

up: env
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) up -d --build
	@$(MAKE) catchup

# Streaming (market-producer, spark-streaming) corre 24/7 por defecto, lo cual
# no tiene sentido para un proyecto academico que no se demuestra todo el dia.
# `catchup` cierra ese hueco: antes de cada demo, trae de Binance solo los
# dias de historico que falten desde la ultima vez registrada (no hace nada
# si ya esta al dia) y dispara el recalculo batch + la reconciliacion.
catchup:
	@echo "Esperando a que Airflow este listo para el catch-up..."
	@until docker exec lambda-airflow airflow jobs check --job-type SchedulerJob >/dev/null 2>&1; do sleep 3; done
	docker exec lambda-airflow python3 -m ingestion.catchup_historical

# Para la presentacion en vivo: a diferencia de `catchup` (silencioso, no hace
# nada si ya esta al dia), esto SIEMPRE dispara y espera una corrida real y
# visible de Batch + reconciliacion, para poder mostrarle al jurado que
# funcionan de verdad sin depender de si habia datos nuevos que traer.
demo:
	docker exec lambda-airflow python3 -m ingestion.catchup_historical --demo

up-all: env
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) --profile llm up -d --build

up-metadata: env
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) --profile metadata up -d

down:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down

restart: down up

ps:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) logs -f

logs-api:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) logs -f api

logs-kafka:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) logs -f kafka

logs-spark:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) logs -f spark-master spark-worker

logs-airflow:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) logs -f airflow

# Escala el clÃºster Spark aÃ±adiendo workers al master ya en marcha.
# El override elimina el `container_name` fijo y publica un rango de puertos
# para que cada rÃ©plica tenga su propia Web UI.
scale-spark:
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_SCALE_FILE) --env-file $(ENV_FILE) \
		up -d --scale spark-worker=$(WORKERS) spark-worker
	@echo "ClÃºster Spark escalado a $(WORKERS) workers. Verifica en http://localhost:$(SPARK_WORKER_WEBUI_PORT)"

# Vuelve a un Ãºnico worker (el comportamiento por defecto de `make up`).
scale-down:
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_SCALE_FILE) --env-file $(ENV_FILE) \
		up -d --scale spark-worker=1 spark-worker


rag-index:
	python -m api.rag_indexer --rebuild

test:
	python -m pytest -q

clean:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down -v --remove-orphans
