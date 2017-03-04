package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import de.kdml.bigdatalab.spark_and_flink.common_utils.LoggerUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.StatisticsUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.flink.utils.FlinkUtils;

/**
 * Compute statistics per trajectory
 * 
 * @author Ehab Qadah
 * 
 *         Jan 2, 2017
 */

public class TrajectoriesStatistics {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = FlinkUtils.getInitializedEnv();

		/**
		 * Reduce the trajectories by aggregate the statics from the previous
		 * trajectory record and keep the new record, the further aggregation
		 * and discard old record.
		 **/

		DataStream<Tuple2<String, PositionMessage>> trajectoriesStream = TrajectoriesStreamUtils
				.getTrajectoriesStream(env)
				.reduce((Tuple2<String, PositionMessage> tuple1, Tuple2<String, PositionMessage> tuple2) -> {
					// compute & aggregate statistics for the new trajectory
					// based on the statistics of old trajectory
					// discard the old trajectory and just keep the new one
					Tuple2<String, PositionMessage> oldTuple = tuple2.f1.isNew() ? tuple1 : tuple2;
					Tuple2<String, PositionMessage> newTuple = tuple2.f1.isNew() ? tuple2 : tuple1;
					StatisticsUtils.computeStatistics(oldTuple.f1, newTuple.f1);
					newTuple.f1.setNew(false);
					return newTuple;
				});

		// showLatecies(trajectoriesStream);
		trajectoriesStream.print().setParallelism(1);

		showThroughput(trajectoriesStream);
		env.execute(" Flink Trajectories Statistics Computation");
	}

	private static void showThroughput(DataStream<Tuple2<String, PositionMessage>> trajectoriesStream) {
		DataStream<Tuple2<String, PositionMessage>> trajectoriesStreamWithTime = trajectoriesStream

				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, PositionMessage>>() {

					/**
					 *
					 * @link{https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/event_timestamps_watermarks.html}
					 */
					private static final long serialVersionUID = 1L;
					private final long maxTimeLag = 1000; // 5 seconds

					@Override
					public long extractTimestamp(Tuple2<String, PositionMessage> element,
							long previousElementTimestamp) {
						return element.f1.getStreamedTime();
					}

					@Override
					public Watermark getCurrentWatermark() {
						// return the watermark as current time minus the
						// maximum time lag
						return new Watermark(System.currentTimeMillis() - maxTimeLag);
					}
				});

		DataStream<Tuple2<String, Integer>> throughput = trajectoriesStreamWithTime.timeWindowAll(Time.seconds(60))

				.fold(Tuple2.of("number_of_elements_per_window", 0),
						new FoldFunction<Tuple2<String, PositionMessage>, Tuple2<String, Integer>>() {

							private static final long serialVersionUID = 3540889174899846578L;

							@Override
							public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator,
									Tuple2<String, PositionMessage> value) throws Exception {
								return Tuple2.of(accumulator.f0, accumulator.f1 + 1);
							}
						});
		throughput.addSink(new SinkFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 6840724449853693597L;

			@Override
			public void invoke(Tuple2<String, Integer> value) throws Exception {
				// log the throughput
				LoggerUtils.logMessage(value.f1.toString());

			}
		});
		throughput.print();
	}

	/**
	 * Show latencies of processed trajectories based on the time delay between
	 * streamed time and finished time
	 * 
	 * @param trajectoriesStream
	 */

	private static void showLatecies(DataStream<Tuple2<String, PositionMessage>> trajectoriesStream) {
		DataStream<Long> latencies = trajectoriesStream.map(tuple -> {

			long currentTime = System.currentTimeMillis();
			// Calculate latency
			PositionMessage position = tuple.f1;
			Long latency = new Long(currentTime - position.getStreamedTime());
			// log the latency
			LoggerUtils.logMessage(latency.toString());
			return latency;
		});

		latencies.print().setParallelism(1);
	}
}
