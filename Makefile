.PHONY: build up down submit logs clean kafka-produce kafka-alerts

# Build the fat jar locally (requires Java 21)
build:
	./gradlew shadowJar

# Start Kafka + Flink cluster (run `make build` first)
up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

# Submit the job to the running Flink cluster
submit:
	docker compose -f docker/docker-compose.yml exec jobmanager \
		flink run -c com.flightanomaly.FlightAnomalyJob \
		/opt/flink/usrlib/flight-anomaly-detector-0.1.0-SNAPSHOT-all.jar

logs:
	docker compose -f docker/docker-compose.yml logs -f taskmanager

logs-jm:
	docker compose -f docker/docker-compose.yml logs -f jobmanager

# Send a test message to Kafka to verify the pipeline works end-to-end
kafka-produce:
	docker compose -f docker/docker-compose.yml exec kafka \
		kafka-console-producer.sh --bootstrap-server localhost:9092 --topic flight-events

# Consume anomaly alerts from Kafka
kafka-alerts:
	docker compose -f docker/docker-compose.yml exec kafka \
		kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomaly-alerts --from-beginning

clean:
	./gradlew clean
	docker compose -f docker/docker-compose.yml down -v
