FROM python:3.11-slim

WORKDIR /app

COPY ingestion/requirements.txt /app/ingestion/requirements.txt
RUN pip install --no-cache-dir -r /app/ingestion/requirements.txt

COPY ingestion /app/ingestion

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ingestion.connectors.market_rest_stream"]
