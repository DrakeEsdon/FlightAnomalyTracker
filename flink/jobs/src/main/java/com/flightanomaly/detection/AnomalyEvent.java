package com.flightanomaly.detection;

import com.flightanomaly.model.FlightEvent;

import java.util.List;
import java.util.UUID;

public record AnomalyEvent(
    String id,
    String scenario,
    String severity,
    String description,
    String icao24,
    String callsign,
    Double latitude,
    Double longitude,
    Double altitude,
    long detectedAt,
    List<FlightEvent> matchedEvents
) {

    public static AnomalyEvent from(String scenario, String severity, String description,
                                     List<FlightEvent> matchedEvents) {
        FlightEvent latest = matchedEvents.get(matchedEvents.size() - 1);
        return new AnomalyEvent(
                UUID.randomUUID().toString(),
                scenario,
                severity,
                description,
                latest.icao24(),
                latest.callsign(),
                latest.latitude(),
                latest.longitude(),
                latest.baroAltitude(),
                System.currentTimeMillis(),
                matchedEvents
        );
    }
}
