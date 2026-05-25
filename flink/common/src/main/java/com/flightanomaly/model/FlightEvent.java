package com.flightanomaly.model;

public record FlightEvent(  
    String callsign,
    String icao24,
    Double latitude,
    Double longitude,
    Double baroAltitude,
    Double velocity,
    Double trueTrack,
    Double verticalRate,
    boolean onGround,
    String squawk,
    long timestamp
) {}