package com.flightanomaly.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flightanomaly.model.FlightEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class FlightEventDeserializer implements DeserializationSchema<FlightEvent> {

    private transient ObjectMapper mapper;

    @Override
    public FlightEvent deserialize(byte[] bytes) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper.readValue(bytes, FlightEvent.class);
    }

    @Override
    public boolean isEndOfStream(FlightEvent event) {
        return false;
    }

    @Override
    public TypeInformation<FlightEvent> getProducedType() {
        return TypeInformation.of(FlightEvent.class);
    }
}
