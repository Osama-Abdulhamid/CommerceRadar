FROM python:3.14-slim

WORKDIR /app

COPY services/api/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/api/clickhouse.py /app/clickhouse.py
COPY services/api/postgres.py /app/postgres.py
COPY services/api/accounts.py /app/accounts.py
COPY services/api/alert_routes.py /app/alert_routes.py
COPY services/api/alert_engine.py /app/alert_engine.py
COPY services/api/main.py /app/main.py

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
