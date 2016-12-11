package de.kdml.bigdatalab.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * This approach uses a Receiver to receive the data. The Receiver is
 * implemented using the Kafka high-level consumer API. As with all receivers,
 * the data received from Kafka through a Receiver is stored in Spark executors,
 * and then jobs launched by Spark Streaming processes the data
 * 
 * @author ehab
 *
 */
public class KafkaReceiverStateful {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Spark Streaming Kafaka  Receiver-based Approach");

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = configs.getStringProp("topics").split(",");
		for (String topic : topics) {
			topicMap.put(topic, 1);
		}

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc,
				configs.getStringProp("zookeeper"), configs.getStringProp("kafkaGroupId"), topicMap,
				StorageLevel.MEMORY_AND_DISK_SER());

		// Get the lines from kafka messages
		JavaDStream<String> lines = messages.map(tuple -> {
			return tuple._2();
		});

		/// Count each word in each batch
		// build the pair (word,count) for all words in lines stream
		JavaPairDStream<String, Integer> wordCounts = lines.flatMapToPair(line -> {

			List<Tuple2<String, Integer>> tuples = new ArrayList<>();
			// create list of tuples of words and their counts
			for (String word : line.split(" ")) {

				tuples.add(new Tuple2<>(word, 1));
			}
			return tuples.iterator();

		}).reduceByKey((i1, i2) -> {
			// Aggregate the word counts
			return i1 + i2;
		});

		// Return a new "state" count DStream where the state for each key(word)
		// is updated
		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

			private static final long serialVersionUID = -76088662409004569L;

			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {

				int sum = state.or(0);
				for (long i : values) {
					sum += i;
				}
				return Optional.of(sum);
			}
		};
		// update running word counts with new batch data
		JavaPairDStream<String, Integer> stateDstream = wordCounts.updateStateByKey(updateFunction);

		stateDstream.print(100);

		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("error:" + e.getMessage());
		}
	}
}
