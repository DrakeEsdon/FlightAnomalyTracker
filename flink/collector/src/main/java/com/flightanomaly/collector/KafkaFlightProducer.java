package com.flightanomaly.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flightanomaly.model.FlightEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class KafkaFlightProducer {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String topic;

    public KafkaFlightProducer(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        this.producer = new KafkaProducer<>(props);
    }

    public void send(List<FlightEvent> events) throws Exception {
        for (FlightEvent event : events) {
            String json = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>(topic, event.icao24(), json));
        }
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
