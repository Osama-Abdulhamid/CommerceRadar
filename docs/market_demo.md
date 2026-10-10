# Market demonstration

## Verified
- 18 schema-valid replay events reached Kafka and ClickHouse.
- Three explicitly defined demo products appear at two simulated stores.
- Each store has nine historical observations.
- Final offers are in stock.
- Final EGP prices, store A / store B:
  - Headphones: 1900 / 1800.
  - Laptop: 29900 / 29800.
  - Phone: 14900 / 14800.
- API history checks passed.
- Three current competitor comparisons passed, each with a 100 EGP gap.
- Power BI export includes price history and competitor comparisons.

## Scope
Demo market sources are simulated, not scraped real stores.
Cross-store product IDs are explicitly assigned by the demo generator.
This comparison does not demonstrate automatic cross-store matching.
Adafruit is a separate real collected source.
Observation history shows recorded observations, not continuous market coverage.

## Reproduce
Generate events:
    python scripts/generate_market_demo.py

Start streaming:
    STREAM_TRIGGER=continuous docker compose up -d spark-streaming

Publish:
    docker compose run --rm --no-deps \
      --volume "$PWD/data/samples:/demo:ro" \
      -e REPLAY_INPUT_PATH=/demo/market_demo.generated.jsonl \
      -e REPLAY_OUTPUT=kafka \
      -e REPLAY_INTERVAL_SECONDS=1 \
      replay-producer

Export:
    python scripts/export_powerbi.py

Regenerating creates new event IDs and observation times.
Replaying an existing file reuses its event IDs.
CSV exports are snapshots, not automatic live refreshes.
