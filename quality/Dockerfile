FROM python:3.12-slim
WORKDIR /app
COPY quality/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY services/api/clickhouse.py /app/clickhouse.py
COPY quality/check_data.py /app/check_data.py
COPY quality/server.py /app/server.py
CMD ["python", "/app/check_data.py"]
