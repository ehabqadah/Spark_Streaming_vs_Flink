package de.kdml.bigdatalab.spark_and_flink.flink.examples;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Network word counts flink's example 
 * 
 * @author Ehab Qadah 
 * 
 * Dec 9, 2016
 */
public class NetworkWordCount {

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//disable system logs
		env.getConfig().disableSysoutLogging();

		// get input data by connecting to the socket
		DataStream<String> lines = env.socketTextStream("localhost", 9999, "\n");

		// parse the data, group it, and aggregate the counts
		DataStream<WordWithCount> wordCounts = lines

				.flatMap((String line, Collector<WordWithCount> out) -> {

					for (String word : line.split("\\s")) {
						out.collect(new WordWithCount(word, 1L));
					}

				})

				.keyBy("word").reduce((a, b) -> {
					return new WordWithCount(a.word, a.count + b.count);
				});

		// print the results with a single thread, rather than in parallel
		wordCounts.print().setParallelism(1);

		env.execute("Network flink WordCount");
	}

	// ------------------------------------------------------------------------

	/**
	 * Data type for words with count
	 */
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
