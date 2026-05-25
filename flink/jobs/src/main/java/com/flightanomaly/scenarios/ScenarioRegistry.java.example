package com.flightanomaly.scenarios;

import com.flightanomaly.scenarios.examples.RapidAltitudeDrop;
import com.flightanomaly.scenarios.examples.SpeedExceedance;

import java.util.List;

public final class ScenarioRegistry {

    private ScenarioRegistry() {}

    public static List<Scenario> loadAll() {
        return List.of(
                new RapidAltitudeDrop(),
                new SpeedExceedance()
        );
    }
}
