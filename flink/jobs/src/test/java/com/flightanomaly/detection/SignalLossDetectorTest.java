package com.flightanomaly.detection;

import com.flightanomaly.model.FlightEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SignalLossDetectorTest {

    private static final long TIMEOUT_MS = 5000;
    private static final OutputTag<SignalLossEvent> TAG = new OutputTag<>("signal-loss") {};

    private KeyedOneInputStreamOperatorTestHarness<String, FlightEvent, FlightEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        SignalLossDetector detector = new SignalLossDetector(TIMEOUT_MS, TAG);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(detector),
                FlightEvent::icao24,
                TypeInformation.of(String.class));
        harness.open();
    }

    @Test
    void passesEventThrough() throws Exception {
        FlightEvent event = testEvent("abc123", 1000L);
        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputStreamRecords())
                .hasSize(1)
                .first()
                .extracting(StreamRecord::getValue)
                .isEqualTo(event);
    }

    @Test
    void firesSignalLossAfterTimeout() throws Exception {
        FlightEvent event = testEvent("abc123", 1000L);
        harness.processElement(new StreamRecord<>(event));

        harness.setProcessingTime(harness.getProcessingTime() + TIMEOUT_MS);

        assertThat(harness.getSideOutput(TAG)).hasSize(1);
        SignalLossEvent loss = harness.getSideOutput(TAG).poll().getValue();
        assertThat(loss.icao24()).isEqualTo("abc123");
        assertThat(loss.timeoutMillis()).isEqualTo(TIMEOUT_MS);
    }

    @Test
    void resetsTimerOnNewEvent() throws Exception {
        harness.processElement(new StreamRecord<>(testEvent("abc123", 1000L)));

        // advance partway through the timeout, then send another event to reset it
        harness.setProcessingTime(3000);
        harness.processElement(new StreamRecord<>(testEvent("abc123", 2000L)));

        // original timer would have fired at 5000, but it was reset to 8000
        harness.setProcessingTime(7000);
        assertThat(harness.getSideOutput(TAG)).isNullOrEmpty();

        // exceed the reset timer
        harness.setProcessingTime(9000);
        assertThat(harness.getSideOutput(TAG)).isNotNull().hasSize(1);
    }

    private FlightEvent testEvent(String icao24, long timestamp) {
        return new FlightEvent("TST001", icao24, 51.5, -0.1, 10000.0, 250.0, 90.0, 0.0, false, null, timestamp);
    }
}
