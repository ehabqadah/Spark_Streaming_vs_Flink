package de.kdml.bigdatalab.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 
 * @author ehab
 *
 */

public final class NetworkWordCount {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		// Create the context with a configured batch size
		SparkConf sparkConf = new SparkConf().setAppName("Network WordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(configs.getIntProp("batchDuration")));

		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(configs.getStringProp("socketHost"),
				configs.getIntProp("socketPort"), StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = -3346379470526384970L;

			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = -4481967467359328639L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = -4270697510664029359L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		wordCounts.print();
		ssc.start();
		ssc.awaitTermination();

	}
}
