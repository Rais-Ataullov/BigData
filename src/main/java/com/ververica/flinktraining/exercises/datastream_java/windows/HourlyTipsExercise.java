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
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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

		DataStream<Tuple3<Long, Long, Float>> tipsPerDriverPerHour = fares
				.map(fare -> {
					long hourStart = fare.getEventTime() - (fare.getEventTime() % 3600000);
					return Tuple3.of(hourStart, fare.driverId, fare.tip);
				})
				.returns(Types.TUPLE(Types.LONG, Types.LONG, Types.FLOAT))
				.keyBy(tuple -> tuple.f1)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.sum(2);

		DataStream<Tuple3<Long, Long, Float>> hourlyMax = tipsPerDriverPerHour
				.keyBy(tuple -> tuple.f0)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.process(new ProcessWindowFunction<Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
					@Override
					public void process(Long key, Context context, Iterable<Tuple3<Long, Long, Float>> elements,
										Collector<Tuple3<Long, Long, Float>> out) throws Exception {
						long maxTips = 0;
						long bestDriver = 0;
						for (Tuple3<Long, Long, Float> element : elements) {
							if (element.f2 > maxTips) {
								maxTips = element.f2.longValue();
								bestDriver = element.f1;
							}
						}
						// Возвращаем timestamp окончания окна, driverId, totalTips
						out.collect(Tuple3.of(context.window().getEnd(), bestDriver, (float) maxTips));
					}
				})
				.returns(Types.TUPLE(Types.LONG, Types.LONG, Types.FLOAT));

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

}
