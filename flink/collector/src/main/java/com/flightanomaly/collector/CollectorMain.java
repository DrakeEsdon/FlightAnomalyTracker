package com.flightanomaly.collector;

import com.flightanomaly.model.FlightEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CollectorMain {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorMain.class);

    public static void main(String[] args) {

        String clientId = System.getenv("OPENSKY_CLIENT_ID");
        String clientSecret = System.getenv("OPENSKY_CLIENT_SECRET");
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BROKERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "flight-events");
        long pollInterval = Long.parseLong(System.getenv().getOrDefault("OPENSKY_POLL_INTERVAL_SECONDS", "300"));

        if (clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank()) {
            LOG.error("OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set (use --env-file .env or env_file in compose)");
            System.exit(1);
        }

        OpenSkyClient openSky = new OpenSkyClient(clientId, clientSecret);
        StateVectorParser parser = new StateVectorParser();
        KafkaFlightProducer producer = new KafkaFlightProducer(kafkaBrokers, topic);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down collector...");
            producer.close();
        }));

        LOG.info("Starting collector: polling every {}s, publishing to {}", pollInterval, topic);

        while (true) {
            long sleepMs = pollInterval * 1000;
            try {
                String json = openSky.fetchStates();
                List<FlightEvent> events = parser.parse(json);
                producer.send(events);
                LOG.info("Published {} flight events", events.size());
            } catch (OpenSkyClient.RateLimitException e) {
                sleepMs = Math.max(sleepMs, e.getWaitMs());
                LOG.warn("Rate limited by OpenSky — waiting {}s before next request", sleepMs / 1000);
            } catch (Exception e) {
                LOG.error("Error during collection cycle", e);
            }

            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
