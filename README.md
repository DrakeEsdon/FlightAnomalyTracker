# Flight Anomaly Detector

Real-time anomaly detection for aircraft using Apache Flink CEP. Consumes live ADS-B telemetry from
[OpenSky Network](https://opensky-network.org), runs it through configurable detection scenarios, and publishes
matched anomalies to Kafka for downstream consumers (dashboards, alerting, etc).

## Prerequisites

- Java 21 (tested with [Eclipse Temurin](https://adoptium.net/))
- Docker & Docker Compose
- Make (optional)

Set `JAVA_HOME` before building:

```bash
# Linux/macOS
export JAVA_HOME=/path/to/jdk-21

# Windows (PowerShell)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
```

## Getting Started

```bash
make build          # builds the fat jar
make up             # starts Kafka + Flink cluster
make submit         # submits the job to Flink
make logs           # tail the task manager output
make kafka-alerts   # consume from the anomaly-alerts topic
make down           # tear down
```

## Project Layout

```
flink/
  common/         shared model (FlightEvent record)
  collector/      polls OpenSky API, publishes to Kafka
  jobs/           Flink streaming job (CEP scenarios, signal loss, sinks)
docker/           docker-compose for local Kafka + Flink
```

## How Scenarios Work

Each anomaly detection scenario implements the `Scenario` interface and returns a Flink CEP `Pattern`.
The `ScenarioRegistry` determines which scenarios are active at runtime.

See `flink/jobs/src/.../scenarios/examples/` for reference implementations.

## Configuration

Environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `flight-events` | Input topic |
| `KAFKA_ALERTS_TOPIC` | `anomaly-alerts` | Output topic for detected anomalies |
| `OPENSKY_CLIENT_ID` | - | OpenSky OAuth2 client ID |
| `OPENSKY_CLIENT_SECRET` | - | OpenSky OAuth2 client secret |
| `OPENSKY_POLL_INTERVAL_SECONDS` | `60` | Polling frequency |
