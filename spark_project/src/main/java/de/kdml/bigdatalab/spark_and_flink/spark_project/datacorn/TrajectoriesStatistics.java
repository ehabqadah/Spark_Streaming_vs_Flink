package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.StatisticsUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;
import scala.Tuple2;

/**
 * 
 * Compute statistics per trajectory
 * 
 * @author Ehab Qadah
 * 
 *         Jan 2, 2017
 */

public class TrajectoriesStatistics {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		// configure spark streaming context
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Trajectories Statistics Computation ");

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		Set<String> topicsSet = new HashSet<String>();
		topicsSet.add(configs.getStringProp("topicId"));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", configs.getStringProp("bootstrap.servers"));
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", configs.getStringProp("kafkaGroupId"));
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		// Create direct kafka stream
		// Create multiple stream and union them
		JavaInputDStream<ConsumerRecord<String, String>> dataStream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));

		// Start the computation

		JavaPairDStream<String, Iterable<Trajectory>> trajectories = dataStream.mapToPair(record -> {

			Trajectory trajectory = Trajectory.parseDataInput(record.value());
			return new Tuple2<>(trajectory.getID(), trajectory);
		}).filter(tuple -> {
			// get only msg2 & msg3 types
			return ("MSG2".equals(tuple._2.getType()) || "MSG3".equals(tuple._2.getType()));
		}).groupByKey();

		// update the stream state
		JavaPairDStream<String, Iterable<Trajectory>> runningTrajectories = trajectories
				.updateStateByKey(updateTrajectoriesStreamFunction);

		// NOTE: mapWithState is still in the experimental phase
		// JavaPairDStream<String, Iterable<Trajectory>> runningTrajectories =
		// trajectories
		// .mapWithState(StateSpec.function(updateTrajectoriesStreamFunction2)).stateSnapshots();
		runningTrajectories.print();
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("Error:" + e.getMessage());
		}
	}

	/**
	 * Return a new "state" DStream where the state for each key is updated by
	 * applying the given function on the previous state of the key and the new
	 * values for the key
	 * 
	 */
	public static Function2<List<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>> updateTrajectoriesStreamFunction = new Function2<List<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>>() {

		private static final long serialVersionUID = -76088662409004569L;

		@Override
		public Optional<Iterable<Trajectory>> call(List<Iterable<Trajectory>> values,
				Optional<Iterable<Trajectory>> state) {

			List<Trajectory> aggregatedTrajectories = new ArrayList<>();
			// add old state
			for (Trajectory trajectory : state.orElse(new ArrayList<>())) {
				aggregatedTrajectories.add(trajectory);
			}

			// aggergate new values

			for (Iterable<Trajectory> val : values) {
				for (Trajectory trajectory : val) {
					aggregatedTrajectories.add(trajectory);
				}

			}

			return Optional.of(StatisticsUtils.computeStatistics(aggregatedTrajectories));
		}
	};

	/**
	 * * Return a new "state" DStream where the state for each key is updated by
	 * applying the given function on the previous state of the key and the new
	 * values for the key
	 * 
	 * To be used with mapWithState
	 */
	public static Function3<String, Optional<Iterable<Trajectory>>, State<Iterable<Trajectory>>, Tuple2<String, Iterable<Trajectory>>> updateTrajectoriesStreamFunction2 = new Function3<String, Optional<Iterable<Trajectory>>, State<Iterable<Trajectory>>, Tuple2<String, Iterable<Trajectory>>>() {

		private static final long serialVersionUID = -1393453967261881632L;

		@Override
		public Tuple2<String, Iterable<Trajectory>> call(String id, Optional<Iterable<Trajectory>> values,
				State<Iterable<Trajectory>> state) throws Exception {
			List<Trajectory> result = new ArrayList<>();
			// add old state
			for (Trajectory trajectory : (state.exists() ? state.get() : new ArrayList<Trajectory>())) {
				result.add(trajectory);
			}

			// aggergate new values

			for (Trajectory val : values.orElse(new ArrayList<Trajectory>())) {

				result.add(val);

			}

			state.update(result);
			return new Tuple2<>(id, result);
		}
	};
}
