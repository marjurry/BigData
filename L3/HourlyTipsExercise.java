/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		// Step 1: Calculate total tips per driver per hour
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
				.keyBy(fare -> fare.driverId)
				.timeWindow(Time.hours(1))
				.aggregate(new SumTipsAggregator(), new TipsWindowFunction());

		// Step 2: Find the maximum tip total in each hour across all drivers
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
				.keyBy(tip -> tip.f0)  // key by window end time
				.timeWindow(Time.hours(1))
				.max(2);  // max by tip amount (3rd field)

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	// AggregateFunction that sums tips per driver
	public static class SumTipsAggregator implements AggregateFunction<TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>> {

		@Override
		public Tuple2<Long, Float> createAccumulator() {
			return new Tuple2<>(0L, 0f);
		}

		@Override
		public Tuple2<Long, Float> add(TaxiFare fare, Tuple2<Long, Float> accumulator) {
			accumulator.f0 = fare.driverId;
			accumulator.f1 += fare.tip;
			return accumulator;
		}

		@Override
		public Tuple2<Long, Float> getResult(Tuple2<Long, Float> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<Long, Float> merge(Tuple2<Long, Float> a, Tuple2<Long, Float> b) {
			return new Tuple2<>(a.f0, a.f1 + b.f1);
		}
	}

	// ProcessWindowFunction to add window timestamp to the result
	public static class TipsWindowFunction extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long key, Context context, Iterable<Tuple2<Long, Float>> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Tuple2<Long, Float> result = elements.iterator().next();
			out.collect(new Tuple3<>(context.window().getEnd(), result.f0, result.f1));
		}
	}
}