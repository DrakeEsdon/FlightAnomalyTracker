package com.flightanomaly.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flightanomaly.model.FlightEvent;

import java.util.ArrayList;
import java.util.List;

public class StateVectorParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public List<FlightEvent> parse(String json) throws Exception {
        JsonNode root = mapper.readTree(json);
        JsonNode states = root.get("states");

        List<FlightEvent> events = new ArrayList<>();

        if (states == null || states.isNull()) {
            return events;
        }

        //parse out the flight events and handling null values
        for (JsonNode state : states) {
            String icao24 = state.get(0).asText();
            String callsign = state.get(1).isNull() ? null : state.get(1).asText().trim();
            Double longitude = state.get(5).isNull() ? null : state.get(5).asDouble();
            Double latitude = state.get(6).isNull() ? null : state.get(6).asDouble();
            Double baroAltitude = state.get(7).isNull() ? null : state.get(7).asDouble();
            boolean onGround = state.get(8).asBoolean();
            Double velocity = state.get(9).isNull() ? null : state.get(9).asDouble();
            Double trueTrack = state.get(10).isNull() ? null : state.get(10).asDouble();
            Double verticalRate = state.get(11).isNull() ? null : state.get(11).asDouble();
            String squawk = state.get(14).isNull() ? null : state.get(14).asText();
            long timestamp = state.get(4).asLong();

            events.add(new FlightEvent(
                    callsign,
                    icao24,
                    latitude,
                    longitude,
                    baroAltitude,
                    velocity,
                    trueTrack,
                    verticalRate,
                    onGround,
                    squawk,
                    timestamp
            ));
        }

        return events;
    }
}
