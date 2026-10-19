# ==============================================================================
# Makefile para la Plataforma Lambda (Datos Financieros y Criptomonedas)
# ==============================================================================

COMPOSE_FILE = infra/docker-compose.yml
ENV_FILE = .env

.PHONY: help env up down restart ps logs logs-api logs-kafka logs-spark logs-airflow clean

help:
	@echo "Comandos disponibles:"
	@echo "  make env           Copia .env.example a .env si no existe"
	@echo "  make up            Levanta la infraestructura base (Kafka, MinIO, Postgres, Spark, API)"
	@echo "  make up-all        Levanta todo el stack incluyendo LLM local (perfil llm)"
	@echo "  make down          Detiene todos los contenedores"
	@echo "  make restart       Reinicia todos los servicios"
	@echo "  make ps            Muestra el estado de los contenedores"
	@echo "  make logs          Muestra logs combinados en tiempo real"
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

clean:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down -v --remove-orphans
