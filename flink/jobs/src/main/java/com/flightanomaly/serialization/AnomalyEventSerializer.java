package com.flightanomaly.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flightanomaly.detection.AnomalyEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class AnomalyEventSerializer implements KafkaRecordSerializationSchema<AnomalyEvent> {

    private final String topic;
    private transient ObjectMapper mapper;

    public AnomalyEventSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(AnomalyEvent event, KafkaSinkContext ctx, Long timestamp) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            byte[] key = event.icao24().getBytes();
            byte[] value = mapper.writeValueAsBytes(event);
            return new ProducerRecord<>(topic, key, value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize AnomalyEvent", e);
        }
    }
}
