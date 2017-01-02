package de.kdml.bigdatalab.spark_and_flink.spark_project.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import de.kdml.bigdatalab.spark_and_flink.spark_project.Configs;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;
import de.kdml.bigdatalab.spark_and_flink.spark_project.WordCountsUtil;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * This approach periodically queries Kafka for the latest offsets in each
 * topic+partition, and accordingly defines the offset ranges to process in each
 * batch
 * 
 * @author ehab
 *
 */

public class KafkaDirect {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		// Create context with a 3 seconds batch interval
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Java DirectKafkaWordCount");

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(configs.getStringProp("topics").split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", configs.getStringProp("brokers"));

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(tuple -> {
			return tuple._2();
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 3664730157250100313L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		}).persist();
		
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

		// save batch word counts and display aggregation result
		WordCountsUtil.aggregateWordCountsAndPrint(sc, wordCounts, configs.getStringProp("KafkaDirect_Out"));

		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("error:" + e.getMessage());
		}
	}
}
