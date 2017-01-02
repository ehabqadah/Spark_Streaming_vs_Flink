package de.kdml.bigdatalab.spark.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import de.kdml.bigdatalab.spark.Configs;
import de.kdml.bigdatalab.spark.SparkConfigsUtils;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaDirectStateful {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		// Create context with a 3 seconds batch interval
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Java DirectKafkaWordCount with update state by key");

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(configs.getStringProp("topics").split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", configs.getStringProp("brokers"));

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

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

		////////////////////

		// Update the cumulative count function
		// Return a new "state" count DStream where the state for each key(word)
		// is updated
		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

			private static final long serialVersionUID = -76088662409004569L;

			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {

				// get state sum and add new values to it
				int sum = state.or(0);
				for (long i : values) {
					sum += i;
				}
				return Optional.of(sum);
			}
		};

		// update running word counts with new batch data
		JavaPairDStream<String, Integer> stateDstream = wordCounts.updateStateByKey(updateFunction);

		stateDstream.foreachRDD(rdd -> {
			System.out.println(rdd.take(100));
		});
		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("error:" + e.getMessage());
		}
	}
}
