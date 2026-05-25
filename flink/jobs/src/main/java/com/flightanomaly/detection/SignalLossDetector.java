package com.flightanomaly.detection;

import com.flightanomaly.model.FlightEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SignalLossDetector extends KeyedProcessFunction<String, FlightEvent, FlightEvent> {

    private final long timeoutMillis;
    private final OutputTag<SignalLossEvent> signalLossOutput;

    private transient ValueState<Long> lastSeenState;

    public SignalLossDetector(long timeoutMillis, OutputTag<SignalLossEvent> signalLossOutput) {
        this.timeoutMillis = timeoutMillis;
        this.signalLossOutput = signalLossOutput;
    }

    private ValueState<Long> getLastSeenState() {
        if (lastSeenState == null) {
            lastSeenState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-seen", Long.class));
        }
        return lastSeenState;
    }

    @Override
    public void processElement(FlightEvent event, Context ctx, Collector<FlightEvent> out) throws Exception {
        Long previousTimer = getLastSeenState().value();
        if (previousTimer != null) {
            ctx.timerService().deleteProcessingTimeTimer(previousTimer);
        }

        long timerTime = ctx.timerService().currentProcessingTime() + timeoutMillis;
        ctx.timerService().registerProcessingTimeTimer(timerTime);
        getLastSeenState().update(timerTime);

        out.collect(event);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FlightEvent> out) throws Exception {
        ctx.output(signalLossOutput, new SignalLossEvent(
                ctx.getCurrentKey(),
                timestamp,
                timeoutMillis
        ));
    }
}
