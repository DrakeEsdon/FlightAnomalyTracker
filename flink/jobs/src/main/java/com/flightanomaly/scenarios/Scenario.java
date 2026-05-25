package com.flightanomaly.scenarios;

import com.flightanomaly.model.FlightEvent;
import org.apache.flink.cep.pattern.Pattern;

public interface Scenario {

    String name();

    String severity();

    String description();

    Pattern<FlightEvent, FlightEvent> build();
}
