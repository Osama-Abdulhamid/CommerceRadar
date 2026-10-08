FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY services/ingestion/requirements.txt ./requirements.txt

RUN python -m pip install --no-cache-dir -r requirements.txt

RUN groupadd --gid 10001 appuser \
    && useradd --uid 10001 --gid appuser --create-home appuser

COPY contracts/v1/product_observation.schema.json \
    ./contracts/v1/product_observation.schema.json

COPY services/ingestion/adafruit.py \
    services/ingestion/adafruit_collector.py \
    ./services/ingestion/

USER 10001:10001

CMD ["python", "services/ingestion/adafruit_collector.py"]
