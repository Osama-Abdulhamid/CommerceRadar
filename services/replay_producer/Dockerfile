FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements-dev.txt ./requirements-dev.txt

RUN python -m pip install --no-cache-dir -r requirements-dev.txt

RUN groupadd --gid 10001 appuser \
    && useradd --uid 10001 --gid appuser --create-home appuser

COPY contracts/v1/product_observation.schema.json \
    ./contracts/v1/product_observation.schema.json

COPY data/samples/product_observations.v1.jsonl \
    ./data/samples/product_observations.v1.jsonl

COPY services/replay_producer/main.py \
    ./services/replay_producer/main.py

USER appuser

CMD ["python", "services/replay_producer/main.py"]
