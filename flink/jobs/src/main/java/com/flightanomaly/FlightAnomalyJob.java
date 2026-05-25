package com.flightanomaly;

import com.flightanomaly.deserialization.FlightEventDeserializer;
import com.flightanomaly.detection.AnomalyEvent;
import com.flightanomaly.detection.SignalLossDetector;
import com.flightanomaly.detection.SignalLossEvent;
import com.flightanomaly.model.FlightEvent;
import com.flightanomaly.scenarios.Scenario;
import com.flightanomaly.scenarios.ScenarioRegistry;
import com.flightanomaly.serialization.AnomalyEventSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlightAnomalyJob {

    private static final long SIGNAL_LOSS_TIMEOUT_MS = 300_000;

    public static final OutputTag<SignalLossEvent> SIGNAL_LOSS_TAG =
            new OutputTag<>("signal-loss") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "kafka:9092");
        String inputTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "flight-events");
        String outputTopic = System.getenv().getOrDefault("KAFKA_ALERTS_TOPIC", "anomaly-alerts");

        List<Scenario> scenarios = ScenarioRegistry.loadAll();

        KafkaSource<FlightEvent> source = KafkaSource.<FlightEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopic)
                .setGroupId("flight-anomaly-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new FlightEventDeserializer())
                .build();

        KafkaSink<AnomalyEvent> alertSink = KafkaSink.<AnomalyEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(new AnomalyEventSerializer(outputTopic))
                .build();

        WatermarkStrategy<FlightEvent> watermarkStrategy = WatermarkStrategy
                .<FlightEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                .withTimestampAssigner((event, ts) -> event.timestamp() * 1000);

        KeyedStream<FlightEvent, String> flightEvents = env
                .fromSource(source, watermarkStrategy, "Kafka Source")
                .keyBy(FlightEvent::icao24);

        SingleOutputStreamOperator<FlightEvent> mainStream = flightEvents
                .process(new SignalLossDetector(SIGNAL_LOSS_TIMEOUT_MS, SIGNAL_LOSS_TAG));

        mainStream.getSideOutput(SIGNAL_LOSS_TAG).print("signal-loss");

        List<DataStream<AnomalyEvent>> anomalyStreams = new ArrayList<>();
        for (Scenario scenario : scenarios) {
            Pattern<FlightEvent, FlightEvent> pattern = scenario.build();

            PatternStream<FlightEvent> patternStream = CEP.pattern(
                    mainStream.keyBy(FlightEvent::icao24), pattern);

            anomalyStreams.add(patternStream.process(new ScenarioMatchHandler(scenario)));
        }

        if (!anomalyStreams.isEmpty()) {
            DataStream<AnomalyEvent> unioned = anomalyStreams.get(0);
            for (int i = 1; i < anomalyStreams.size(); i++) {
                unioned = unioned.union(anomalyStreams.get(i));
            }
            unioned.sinkTo(alertSink).name("Kafka Anomaly Alerts");
            unioned.print("anomaly-alerts");
        }

        env.execute("Flight Anomaly Detector");
    }

    private static class ScenarioMatchHandler extends PatternProcessFunction<FlightEvent, AnomalyEvent> {

        private final String name;
        private final String severity;
        private final String description;

        ScenarioMatchHandler(Scenario scenario) {
            this.name = scenario.name();
            this.severity = scenario.severity();
            this.description = scenario.description();
        }

        @Override
        public void processMatch(Map<String, List<FlightEvent>> match, Context ctx, Collector<AnomalyEvent> out) {
            List<FlightEvent> allEvents = new ArrayList<>();
            match.values().forEach(allEvents::addAll);
            out.collect(AnomalyEvent.from(name, severity, description, allEvents));
        }
    }
}
