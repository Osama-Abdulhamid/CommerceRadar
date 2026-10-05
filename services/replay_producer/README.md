# Replay Producer

Replays saved product observations at a configurable interval.

## Current Behavior

1. Reads the input JSONL file.
2. Validates the schema and all input events.
3. Rejects duplicate event IDs within the input file.
4. Prints each event to standard output.
5. Waits between events, then exits after one pass.

Events retain their original event_id and observed_at.
Replaying an event does not create a new observation.

Docker execution is implemented and verified.
Kafka delivery is not implemented yet.

## Local Execution

Run from the project root with the virtual environment activated
and requirements-dev.txt installed:

```bash
python services/replay_producer/main.py
```

Expected:
- Five validated sample events.
- One second between events by default.
- A completion message after the final event.

Change the interval for one execution:

```bash
REPLAY_INTERVAL_SECONDS=0.5 python services/replay_producer/main.py
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| LOG_LEVEL | INFO | Logging verbosity |
| SCHEMA_VERSION | 1.0 | Expected contract version |
| REPLAY_INPUT_PATH | data/samples/product_observations.v1.jsonl | Input file |
| REPLAY_INTERVAL_SECONDS | 1 | Delay between events |

Relative input paths are resolved against the project root.
Absolute input paths are also supported.

The program reads process environment variables.
It does not automatically load .env.

The current schema file is fixed to:
contracts/v1/product_observation.schema.json

Setting SCHEMA_VERSION does not select another schema file.
It must match the version defined in the current schema.

## Output and Logging

- stdout: one JSON event per line.
- stderr: operational logs and errors.
- The first event is emitted immediately after validation.
- The interval controls replay speed, not original observation timing.

## Failure Behavior

The program exits without emitting events if input validation fails.

Examples:
- Missing or unreadable input file.
- Invalid JSON or schema violations.
- Duplicate event IDs.
- Unsupported schema version.
- Invalid logging level.
- Negative or non-finite replay interval.

Exit codes:
- 0: successful completion.
- 1: configuration, input, or validation failure.
- 130: interrupted with Ctrl+C.

## Verified Locally

- Default replay: five events with a one-second interval.
- Configured replay: five events with a half-second interval.
- Negative interval: rejected before emitting any events.

## Limitations

This initial version loads all input events into memory before replay.
Use it for small development samples, not the full historical dataset.

It does not provide Kafka delivery, retries, continuous looping,
or pipeline-wide deduplication.


## Docker Execution

Run these commands from the project root.

Build the image:

```bash
docker compose build replay-producer
```

Run a temporary container:

```bash
docker compose run --rm replay-producer
```

Compose passes the configured environment variables to the container.
The image includes the schema and the synthetic sample dataset.

Expected:
- Five events emitted with a one-second interval by default.
- A completion message.
- Exit code 0.

The container runs as a non-root user.
The --rm option removes the temporary container after it exits;
the image remains available.

Rebuild the image after changing the code, schema, dependencies,
or bundled sample data.

Docker execution was verified successfully with the five sample events.
