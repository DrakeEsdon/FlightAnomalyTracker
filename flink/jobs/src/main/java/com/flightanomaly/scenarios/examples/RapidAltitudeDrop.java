package com.flightanomaly.scenarios.examples;

import com.flightanomaly.model.FlightEvent;
import com.flightanomaly.scenarios.Scenario;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

public class RapidAltitudeDrop implements Scenario {

    @Override
    public String name() {
        return "rapid-altitude-drop";
    }

    @Override
    public String severity() {
        return "HIGH";
    }

    @Override
    public String description() {
        return "Aircraft drops more than 5000m altitude within 60 seconds";
    }

    @Override
    public Pattern<FlightEvent, FlightEvent> build() {
        return Pattern.<FlightEvent>begin("high")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(FlightEvent event) {
                        return event.baroAltitude() != null && event.baroAltitude() >= 10000;
                    }
                })
                .followedBy("low")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(FlightEvent event) {
                        return event.baroAltitude() != null && event.baroAltitude() <= 5000;
                    }
                })
                .within(Duration.ofSeconds(60));
    }
}
