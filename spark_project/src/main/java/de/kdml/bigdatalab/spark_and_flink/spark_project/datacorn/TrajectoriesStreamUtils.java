package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.StreamRecord;
import scala.Tuple2;

/**
 * Trajectories Stream setup
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class TrajectoriesStreamUtils {

	private static Configs configs = Configs.getInstance();

	public static JavaPairDStream<String, Iterable<PositionMessage>> getTrajectoriesStream(JavaStreamingContext jssc) {

		// setup Kafka parameters
		Set<String> topicsSet = new HashSet<String>();
		topicsSet.add(configs.getStringProp("topicId"));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", configs.getStringProp("bootstrap.servers"));
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", configs.getStringProp("kafkaGroupId"));
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		List<JavaDStream<ConsumerRecord<String, String>>> kafkaStreams = new ArrayList<>();

		/**
		 * Create parallel receivers for the same topic within the same group,
		 * in order to scale with Kafka partitions
		 **/
		int numParallelKafkaStream = configs.getIntProp("numberOfKafkParallelStreams");
		for (int i = 0; i < numParallelKafkaStream; i++) {
			// Create direct Kafka stream
			JavaDStream<ConsumerRecord<String, String>> dataStreami = KafkaUtils.createDirectStream(jssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));

			kafkaStreams.add(dataStreami);
		}

		// union all parallel Kafka streams, and use the reslut stream as the
		// main stream
		JavaDStream<ConsumerRecord<String, String>> dataStream = numParallelKafkaStream == 1 ? kafkaStreams.get(0)
				: jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

		// Start the computation
		/**
		 * 1- parse the stream record to create
		 * tuple<trajectory_id,position_message> 2-filter irrelevant tuples
		 * based on position message type 3- group all tuples that share the
		 * same trajectory_id by using groupBy operation ( groubByKey -> Return
		 * a new DStream by applying `groupByKey` on each RDD of `this` DStream.
		 * Therefore, the values for each key in `this` DStream's RDDs are
		 * grouped into a single sequence to generate the RDDs of the new
		 * DStream. org.apache.spark.Partitioner is used to control the
		 * partitioning of each RDD)
		 */
		JavaPairDStream<String, Iterable<PositionMessage>> trajectories = dataStream.mapToPair(record -> {

			// parse the ADBS-B messages from the Kafka stream
			StreamRecord streamRecord = StreamRecord.parseData(record.value());
			PositionMessage trajectory = TrajectoriesUtils.parseDataInput(streamRecord.getValue());
			trajectory.setStreamedTime(streamRecord.getStreamedTime());
			trajectory.setNew(true);

			// create a tuple of Id & position message for each stream line
			return new Tuple2<>(trajectory.getID(), trajectory);
		}).filter(tuple -> {
			// filter only msg2 & msg3 types
			return ("MSG2".equals(tuple._2.getType()) || "MSG3".equals(tuple._2.getType()));
		}).groupByKey();
		// Group all message belong to same aircraft as trajectory
		return trajectories;
	}

	/**
	 * Get last position message in the previous batch's positions
	 */
	public static PositionMessage getLastPositionInBatch(Iterable<PositionMessage> prevBatchPositions) {
		PositionMessage lastOldPosition = null;
		for (PositionMessage trajectory : prevBatchPositions) {
			lastOldPosition = trajectory;
			lastOldPosition.setNew(false);
		}
		return lastOldPosition;
	}

	/**
	 * Aggregate positions of a trajectory and sort them
	 * 
	 * @param newPostitions
	 * @return
	 */
	public static List<PositionMessage> getNewAggregatedPositions(List<Iterable<PositionMessage>> newPostitions) {
		// Combine all new batch positions
		List<PositionMessage> aggregatedPositions = new ArrayList<>();
		// aggregate new values
		for (Iterable<PositionMessage> val : newPostitions) {
			for (PositionMessage position : val) {
				aggregatedPositions.add(position);
			}
		}
		// Sort the new positions based on create time to preserve the
		// correct order of message order
		return TrajectoriesUtils.sortPositionsOfTrajectory(aggregatedPositions);
	}
}
