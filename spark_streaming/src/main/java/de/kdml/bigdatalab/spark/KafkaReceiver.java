package de.kdml.bigdatalab.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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
public class KafkaReceiver {

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

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(tuple -> {
			return tuple._2();
		});
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -3551637796904462300L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 6817408734395025461L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 6398638714793728848L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		WordCountsUtil.aggregateWordCountsAndPrint(sc, wordCounts, configs.getStringProp("KafkaReceiver_Out"));

		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("error:" + e.getMessage());
		}
	}
}
