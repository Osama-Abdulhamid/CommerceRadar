# CommerceRadar Monitoring

Prometheus: http://localhost:19090
Grafana: http://localhost:13000
Dashboard: CommerceRadar / CommerceRadar Operations

Grafana username: admin.
Password is stored locally in .env. Never commit or share it.

Start:
    docker compose up -d --build api prometheus grafana

Prometheus scrapes API metrics and itself every 15 seconds.
Metrics include request counts, response statuses, request duration
and Python process metrics.

Route labels use route templates rather than product IDs or user IDs.
The /metrics endpoint is excluded from request counters.

Grafana provisions the Prometheus datasource and fourteen dashboard panels
from versioned files. Dashboard configuration is read-only in the UI.
Edit the repository JSON to change it.

Metrics persist in prometheus-data, with retention capped at seven days
or 512 MB, whichever is reached first.
Grafana settings persist in grafana-data.

Scope:
- API scrape availability.
- Request rates and server error rates.
- Request latency and API process memory.
- No dedicated Kafka, Spark, HDFS or PostgreSQL exporters yet.
- Scrape availability does not prove complete pipeline health.
- No monitoring notification delivery is configured.

Business analytics and Power BI remain separate from this dashboard.

## Expanded dashboard verification

The dashboard contains 14 panels covering API scrape health, uptime,
request counts and rates, client and server errors, HTTP statuses,
latency percentiles, mean latency by route, process memory and CPU.

A manual traffic check completed 40 read requests, all with HTTP 200.
This verifies the demo requests; it is not a stress or capacity test.

Uptime uses a fixed green display color.
The server error ratio uses a scale from 0 to 1, displayed as a percentage.
Process metrics describe the API process, not the entire Docker host.
