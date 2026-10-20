FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# El paquete api/ se copia como /app/api para que los imports relativos
# funcionen con uvicorn api.main:app.
COPY requirements.txt /app/api/requirements.txt
RUN pip install --no-cache-dir -r /app/api/requirements.txt

COPY . /app/api

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV FRONTEND_DIR=/app/static
ENV STATIC_DIR=/app/static

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
