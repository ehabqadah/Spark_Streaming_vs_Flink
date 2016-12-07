package de.kdml.bigdatalab.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaDirectStateful {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		// Create context with a 3 seconds batch interval
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Java DirectKafkaWordCount with update state by key");

		List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("ehab", 1));
		JavaPairRDD<String, Integer> initialRDD = sc.parallelizePairs(tuples);

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(configs.getStringProp("topics").split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", configs.getStringProp("brokers"));

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 6552316508278748327L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 3664730157250100313L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		}).persist();
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 4319033885138979261L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1761086467757826981L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		////////////////////

		// Update the cumulative count function

		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {

				int sum = state.or(0);
				for (long i : values) {
					sum += i;
				}
				return Optional.of(sum);
			}
		};

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
