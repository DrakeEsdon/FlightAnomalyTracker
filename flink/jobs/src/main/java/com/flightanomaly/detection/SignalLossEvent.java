package com.flightanomaly.detection;

public record SignalLossEvent(
    String icao24,
    long detectedAt,
    long timeoutMillis
) {}
