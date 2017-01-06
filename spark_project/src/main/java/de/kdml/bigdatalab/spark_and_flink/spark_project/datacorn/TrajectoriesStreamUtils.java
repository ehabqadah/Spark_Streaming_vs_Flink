package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
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

	public static JavaPairDStream<String, Iterable<Trajectory>> getTrajectoriesStream(JavaStreamingContext jssc) {

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

		/**
		 * 1- parse data lines to create tuple<id,trajectory>
		 * 
		 * 2-filter messages
		 * 
		 * 3- group all trajectories with same id
		 * 
		 * ( groubByKey -> Return a new DStream by applying `groupByKey` on each
		 * RDD of `this` DStream. Therefore, the values for each key in `this`
		 * DStream's RDDs are grouped into a single sequence to generate the
		 * RDDs of the new DStream. org.apache.spark.Partitioner is used to
		 * control the partitioning of each RDD)
		 */
		JavaPairDStream<String, Iterable<Trajectory>> trajectories = dataStream.mapToPair(record -> {

			Trajectory trajectory = TrajectoriesUtils.parseDataInput(record.value());
			return new Tuple2<>(trajectory.getID(), trajectory);
		}).filter(tuple -> {
			// get only msg2 & msg3 types
			return ("MSG2".equals(tuple._2.getType()) || "MSG3".equals(tuple._2.getType()));
		}).groupByKey();

		return trajectories;
	}

}
