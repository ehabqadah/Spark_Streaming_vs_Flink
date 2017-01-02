package de.kdml.bigdatalab.spark_and_flink.spark_project.examples;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;

/**
 * 
 * @author ehab
 *
 */
public final class StructuredNetworkWordCount {

	private static Configs configs = Configs.getInstance();
	
  public static void main(String[] args) throws Exception {
 

    SparkSession spark = SparkSession
      .builder()
      .appName("StructuredNetworkWordCount")
      .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<String> lines = spark
      .readStream()
      .format("socket")
      .option("host", configs.getStringProp("socketHost"))
      .option("port", configs.getIntProp("socketPort"))
      .load().as(Encoders.STRING());

    // Split the lines into words
    Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
     
		private static final long serialVersionUID = -7711468726452799792L;

	@Override
      public Iterator<String> call(String x) {
        return Arrays.asList(x.split(" ")).iterator();
      }
    }, Encoders.STRING());

		// Generate running word count
		Dataset<Row> wordCounts = words.groupBy("value").count();
		// wordCounts.write().saveAsTable("wordCounts");
		// Start running the query that prints the running counts to the console
		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		query.awaitTermination();
  }
}