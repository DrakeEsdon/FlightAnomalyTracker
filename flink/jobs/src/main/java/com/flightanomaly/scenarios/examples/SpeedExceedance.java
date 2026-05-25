package com.flightanomaly.scenarios.examples;

import com.flightanomaly.model.FlightEvent;
import com.flightanomaly.scenarios.Scenario;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

public class SpeedExceedance implements Scenario {

    @Override
    public String name() {
        return "speed-exceedance";
    }

    @Override
    public String severity() {
        return "MEDIUM";
    }

    @Override
    public String description() {
        return "Aircraft exceeds 340 m/s ground speed (typical max for commercial aviation)";
    }

    @Override
    public Pattern<FlightEvent, FlightEvent> build() {
        return Pattern.<FlightEvent>begin("normal")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(FlightEvent event) {
                        return event.velocity() != null && event.velocity() <= 340;
                    }
                })
                .followedBy("excessive")
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(FlightEvent event) {
                        return event.velocity() != null && event.velocity() > 340;
                    }
                })
                .within(Duration.ofSeconds(30));
    }
}
