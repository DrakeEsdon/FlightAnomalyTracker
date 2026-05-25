package com.flightanomaly.collector;

import com.flightanomaly.model.FlightEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CollectorMain {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorMain.class);

    public static void main(String[] args) {

        //get the environment variables
        String clientId = System.getenv("OPENSKY_CLIENT_ID");
        String clientSecret = System.getenv("OPENSKY_CLIENT_SECRET");
        String kafkaBrokers = System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:9094");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "flight-events");
        long pollInterval = Long.parseLong(System.getenv().getOrDefault("OPENSKY_POLL_INTERVAL_SECONDS", "10"));

        //create the clients
        OpenSkyClient openSky = new OpenSkyClient(clientId, clientSecret);
        StateVectorParser parser = new StateVectorParser();
        KafkaFlightProducer producer = new KafkaFlightProducer(kafkaBrokers, topic);

        //add a shutdown hook to close the producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down collector...");
            producer.close();
        }));

        LOG.info("Starting collector: polling every {}s, publishing to {}", pollInterval, topic);
        
        //start the polling loop
        while (true) {
            try {
                String json = openSky.fetchStates();
                List<FlightEvent> events = parser.parse(json);
                producer.send(events);
                LOG.info("Published {} flight events", events.size());
            } catch (Exception e) {
                LOG.error("Error during collection cycle", e);
            }

            try {
                Thread.sleep(pollInterval * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
