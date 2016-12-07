package de.kdml.bigdatalab.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
		SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("Java DirectKafkaWordCount");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(configs.getIntProp("batchDuration")));

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
		
		//save batch word counts and display aggregation result 
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
