# Flight Anomaly Detector

Real-time anomaly detection for aircraft using Apache Flink CEP. Consumes live ADS-B telemetry from
[OpenSky Network](https://opensky-network.org), runs it through configurable detection scenarios, and publishes
matched anomalies to Kafka for downstream consumers (dashboards, alerting, etc).

## Live demo

A simple live demo is running at **[https://flights.esdon.dev](https://flights.esdon.dev)** — a map and sidebar that show stateful and non-stateful detected anomalies as they arrive from live data. It is meant as a visual representation of the repo, not a production-grade deployment. The active detection rules are intentionally basic example scenarios, not a full operational rule set.

## Prerequisites

**To run the stack:**

- Docker & Docker Compose
- A `.env` file in the project root (copy from `.env.example` and add your OpenSky credentials)

No Java or Gradle required on the host — the Flink job JAR is built inside Docker.

**For local Java development only:** Java 17+ and Gradle (via `./gradlew`).

## Getting Started

```bash
cp .env.example .env   # edit with OpenSky credentials
docker compose -f docker/docker-compose.yml up -d --build
```

- Flink dashboard: http://localhost:8081
- Alerts are published to the `anomaly-alerts` Kafka topic (consume with your own tooling)

```bash
docker compose -f docker/docker-compose.yml down
```

Or with Make:

```bash
make up
make logs          # taskmanager
make logs-collector
make down
```

Submit the Flink job manually (after the stack is up):

```bash
docker compose -f docker/docker-compose.yml --profile manual-submit run --rm submit-job
```

**Local Java builds** (optional): `./gradlew shadowJar` needs JDK 17+ (`JAVA_HOME` must not point at Java 8).

## Project Layout

```
flink/
  common/         shared model (FlightEvent record)
  collector/      polls OpenSky API, publishes to Kafka
  jobs/           Flink streaming job (CEP scenarios, signal loss, sinks)
docker/           docker-compose for local Kafka + Flink + collector
```

## How Scenarios Work

Each anomaly detection scenario implements the `Scenario` interface and returns a Flink CEP `Pattern`.
The `ScenarioRegistry` determines which scenarios are active at runtime.

See `flink/jobs/src/.../scenarios/examples/` for reference implementations.

## Configuration

Environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `kafka:9092` | Kafka bootstrap servers (in Docker) |
| `KAFKA_TOPIC` | `flight-events` | Input topic |
| `KAFKA_ALERTS_TOPIC` | `anomaly-alerts` | Output topic for detected anomalies |
| `OPENSKY_CLIENT_ID` | - | OpenSky OAuth2 client ID |
| `OPENSKY_CLIENT_SECRET` | - | OpenSky OAuth2 client secret |
| `OPENSKY_POLL_INTERVAL_SECONDS` | `300` | Polling frequency |
| `OPENSKY_BBOX` | - | Bounding box `lamin,lomin,lamax,lomax` (omit for global) |
