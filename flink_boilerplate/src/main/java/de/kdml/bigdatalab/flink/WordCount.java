package de.kdml.bigdatalab.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = env.readTextFile("/usr/local/spark/spark-2.0.1-bin-hadoop2.7/README.md");

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
						// group by the tuple field "0" and sum up tuple field
						// "1"
						.groupBy(0).aggregate(Aggregations.SUM, 1);

		// emit result
		counts.print();
	}
}
