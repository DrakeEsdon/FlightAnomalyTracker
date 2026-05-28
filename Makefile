.PHONY: build up down logs logs-collector logs-all clean

COMPOSE = docker compose -f docker/docker-compose.yml

# Build Flink fat jar locally (optional — `make up` builds it in Docker)
build:
	./gradlew shadowJar

# Start full stack; Flink jar is built inside Docker (no host Java required)
up:
	$(COMPOSE) up -d --build

# Stop everything
down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f taskmanager

logs-collector:
	$(COMPOSE) logs -f collector

logs-all:
	$(COMPOSE) logs -f

clean:
	$(COMPOSE) down -v
	./gradlew clean 2>/dev/null || true
