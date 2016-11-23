package de.kdml.bigdatalab.spark;

import org.apache.spark.api.java.*;

import java.util.HashMap;
import java.util.HashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;

public class App {

	public static final AtomicReference<JavaPairDStream<String, Integer>> wordsStream = new AtomicReference<JavaPairDStream<String, Integer>>(
			null);

	// Stats will be computed for the last window length of time.
	private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
	// Stats will be computed every slide interval time.
	private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

	public static void main(String[] args) {

		String brokers = "localhost:9092";
		String topics = "test";

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//
		// List<Tuple2<String, Integer>> input = new ArrayList();
		// input.add(new Tuple2("coffee", 1));
		// input.add(new Tuple2("coffee", 2));
		// input.add(new Tuple2("pandas", 3));
		// JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(input);
		// JavaPairRDD<Text, IntWritable> result2 = rdd2.mapToPair(new
		// ConvertToWritableTypes());
		// result2.saveAsHadoopFile("_out/output3", Text.class,
		// IntWritable.class, SequenceFileOutputFormat.class);
		// JavaPairRDD<Text, IntWritable> input =
		// sc.sequenceFile("_out/output4/part-00000", Text.class,
		// IntWritable.class);
		// JavaPairRDD<String, Integer> result2 = input.mapToPair(new
		// ConvertToNativeTypes());
		// List<Tuple2<String, Integer>> resultList = result2.collect();
		// for (Tuple2<String, Integer> record : resultList) {
		// System.out.println(record);
		// }
		//
		// System.exit(0);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(2));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 7297067362793355972L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		}).persist();
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.foreachRDD((rdd, time) -> {

			if (rdd.isEmpty())
				return;

			JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
			result.saveAsHadoopFile("_out/output5/" + time.toString(), Text.class, IntWritable.class,
					SequenceFileOutputFormat.class);
			rdd.saveAsTextFile("_out/output6");

		});

		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
		public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
			return new Tuple2(new Text(record._1), new IntWritable(record._2));
		}
	}

	public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {
		public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) {
			return new Tuple2(record._1.toString(), record._2.get());
		}
	}
}
