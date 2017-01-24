package de.kdml.bigdatalab.spark_and_flink.flink.examples;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Network word counts flink's example
 * 
 * @author Ehab Qadah
 * 
 *         Dec 9, 2016
 */
public class NetworkWordCount {

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// disable system logs
		env.getConfig().disableSysoutLogging();

		// get input data by connecting to the socket
		DataStream<String> lines = env.socketTextStream("localhost", 9999, "\n");

		DataStream<Tuple2<String, Integer>> counts = // split up the lines in
				// pairs (2-tuples)
				// containing: (word,1)
				lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
					// normalize and split the line into words
					String[] tokens = line.toLowerCase().split("\\W+");

					// emit the pairs
					for (String token : tokens) {
						if (token.length() > 0) {
							out.collect(new Tuple2<String, Integer>(token, 1));
						}
					}

				}).keyBy(0).sum(1);
		
		// number of element per window
		DataStream<Tuple2<String, Integer>> throughput = counts.timeWindowAll(Time.seconds(60)).fold(
				Tuple2.of("number_of_elements_per_window", 0),
				new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> acc, Tuple2<String, Integer> o)
							throws Exception {
						return Tuple2.of(acc.f0, acc.f1 + 1);
					}
				});
		throughput.print().setParallelism(1);
		// print the results with a single thread, rather than in parallel
		// counts.print().setParallelism(1);

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
