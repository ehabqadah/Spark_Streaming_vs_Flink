package de.kdml.bigdatalab.spark_and_flink.spark_project.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;
import scala.Tuple2;

/***
 * This example of socket stream processing in spark
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */

public final class NetworkWordCount {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		// Create the context with a configured batch size
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Network spark WordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(configs.getIntProp("batchDuration")));

		// create line input stream from socket
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(configs.getStringProp("socketHost"),
				configs.getIntProp("socketPort"), StorageLevels.MEMORY_AND_DISK_SER);

		//// Count each word in each batch
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

		wordCounts.print();
		ssc.start();
		ssc.awaitTermination();

	}
}
