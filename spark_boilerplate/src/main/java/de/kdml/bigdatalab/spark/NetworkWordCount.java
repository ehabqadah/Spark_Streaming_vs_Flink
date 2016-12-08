package de.kdml.bigdatalab.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Array;
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
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Network WordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(configs.getIntProp("batchDuration")));

		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(configs.getStringProp("socketHost"),
				configs.getIntProp("socketPort"), StorageLevels.MEMORY_AND_DISK_SER);

		JavaPairDStream<String, Integer> wordCounts2 = lines
				.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

					private static final long serialVersionUID = 3177427441691181201L;

					@Override
					public Iterator<Tuple2<String, Integer>> call(String t) throws Exception {

						List<Tuple2<String, Integer>> tuples = new ArrayList<>();
						// create list of tuples of words and their counts
						for (String word : t.split(" ")) {

							tuples.add(new Tuple2<>(word, 1));
						}
						return tuples.iterator();

					}
				}).reduceByKey((i1, i2) -> {

					return i1 + i2;
				});

		wordCounts2.print();
		ssc.start();
		ssc.awaitTermination();

	}
}
