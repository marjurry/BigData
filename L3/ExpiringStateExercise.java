/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The "Expiring State" exercise from the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class ExpiringStateExercise extends ExerciseBase {
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;           // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
				.keyBy(fare -> fare.rideId);

		SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> processed = rides
				.connect(fares)
				.process(new EnrichmentFunction());

		// Output unmatched fares to side output
		processed.getSideOutput(unmatchedFares).print();

		env.execute("ExpiringStateExercise (java)");
	}

	public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		private transient ValueState<TaxiRide> rideState;
		private transient ValueState<TaxiFare> fareState;
		private transient ValueState<Long> rideTimerState;
		private transient ValueState<Long> fareTimerState;

		private static final long TIMEOUT_MS = 120000; // 2 minutes timeout

		@Override
		public void open(Configuration config) throws Exception {
			rideState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("saved fare", TaxiFare.class));
			rideTimerState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("ride timer", Long.class));
			fareTimerState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("fare timer", Long.class));
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Check which timer fired and emit to appropriate side output
			if (rideTimerState.value() != null && rideTimerState.value() == timestamp) {
				// Ride timer fired - ride was never matched
				TaxiRide ride = rideState.value();
				if (ride != null) {
					ctx.output(unmatchedRides, ride);
				}
				cleanup();
			} else if (fareTimerState.value() != null && fareTimerState.value() == timestamp) {
				// Fare timer fired - fare was never matched
				TaxiFare fare = fareState.value();
				if (fare != null) {
					ctx.output(unmatchedFares, fare);
				}
				cleanup();
			}
		}

		@Override
		public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Check if we already have the fare for this ride
			TaxiFare fare = fareState.value();

			if (fare != null) {
				// Fare arrived earlier, emit the pair and clear state
				fareState.clear();
				clearTimer(fareTimerState);
				out.collect(new Tuple2<>(ride, fare));
			} else {
				// Store the ride and set a timer
				rideState.update(ride);
				long timerTimestamp = context.timestamp() + TIMEOUT_MS;
				context.timerService().registerEventTimeTimer(timerTimestamp);
				rideTimerState.update(timerTimestamp);
			}
		}

		@Override
		public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Check if we already have the ride for this fare
			TaxiRide ride = rideState.value();

			if (ride != null) {
				// Ride arrived earlier, emit the pair and clear state
				rideState.clear();
				clearTimer(rideTimerState);
				out.collect(new Tuple2<>(ride, fare));
			} else {
				// Store the fare and set a timer
				fareState.update(fare);
				long timerTimestamp = context.timestamp() + TIMEOUT_MS;
				context.timerService().registerEventTimeTimer(timerTimestamp);
				fareTimerState.update(timerTimestamp);
			}
		}

		private void clearTimer(ValueState<Long> timerState) throws Exception {
			Long timer = timerState.value();
			if (timer != null) {
				timerState.clear();
			}
		}

		private void cleanup() throws Exception {
			rideState.clear();
			fareState.clear();
			clearTimer(rideTimerState);
			clearTimer(fareTimerState);
		}
	}
}