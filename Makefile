# ==============================================================================
# Makefile para la Plataforma Lambda (Datos Financieros y Criptomonedas)
# ==============================================================================

COMPOSE_FILE = infra/docker-compose.yml
COMPOSE_SCALE_FILE = infra/docker-compose.scale.yml
ENV_FILE = .env

# Número de workers Spark del clúster. Cada worker aporta
# SPARK_WORKER_CORES cores y SPARK_WORKER_MEMORY de memoria.
WORKERS ?= 3

.PHONY: help env up down restart ps logs logs-api logs-kafka logs-spark logs-airflow clean scale-spark scale-down

help:
	@echo "Comandos disponibles:"
	@echo "  make env           Copia .env.example a .env si no existe"
	@echo "  make up            Levanta la infraestructura base (Kafka, MinIO, Postgres, Spark, API)"
	@echo "  make up-all        Levanta todo el stack incluyendo LLM local (perfil llm)"
	@echo "  make down          Detiene todos los contenedores"
	@echo "  make restart       Reinicia todos los servicios"
	@echo "  make ps            Muestra el estado de los contenedores"
	@echo "  make logs          Muestra logs combinados en tiempo real"
	@echo "  make scale-spark   Escala el clúster Spark a WORKERS workers (por defecto 3)"
	@echo "  make clean         Detiene y elimina volúmenes y redes creadas"

env:
	@if [ ! -f $(ENV_FILE) ]; then \
		cp .env.example $(ENV_FILE); \
		echo "Archivo $(ENV_FILE) generado desde .env.example"; \
	else \
		echo "Archivo $(ENV_FILE) ya existe."; \
	fi

up: env
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) up -d --build

up-all: env
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) --profile llm up -d --build

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

# Escala el clúster Spark añadiendo workers al master ya en marcha.
# El override elimina el `container_name` fijo y publica un rango de puertos
# para que cada réplica tenga su propia Web UI.
scale-spark:
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_SCALE_FILE) --env-file $(ENV_FILE) \
		up -d --scale spark-worker=$(WORKERS) spark-worker
	@echo "Clúster Spark escalado a $(WORKERS) workers. Verifica en http://localhost:$(SPARK_WORKER_WEBUI_PORT)"

# Vuelve a un único worker (el comportamiento por defecto de `make up`).
scale-down:
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_SCALE_FILE) --env-file $(ENV_FILE) \
		up -d --scale spark-worker=1 spark-worker

clean:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down -v --remove-orphans
