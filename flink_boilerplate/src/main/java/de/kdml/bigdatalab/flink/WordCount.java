package de.kdml.bigdatalab.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
/**
 * This an example of flink dataset api to read text file and calculate the
 * word counts
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = env.readTextFile("/usr/local/spark/spark-2.0.1-bin-hadoop2.7/README.md");

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
					// normalize and split the line into words
					String[] tokens = line.toLowerCase().split("\\W+");

					// emit the pairs
					for (String token : tokens) {
						if (token.length() > 0) {
							out.collect(new Tuple2<String, Integer>(token, 1));
						}
					}

				})
						// group by the tuple field "0" and sum up tuple field
						// "1"
						.groupBy(0).aggregate(Aggregations.SUM, 1);

		// emit result
		counts.print();
	}
}
