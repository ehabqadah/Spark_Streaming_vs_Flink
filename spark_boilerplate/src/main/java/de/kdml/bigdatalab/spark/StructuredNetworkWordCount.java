package de.kdml.bigdatalab.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;


public final class StructuredNetworkWordCount {

  public static void main(String[] args) throws Exception {
 

    String host = "localhost";
    int port = 9999;

    SparkSession spark = SparkSession
      .builder()
      .appName("StructuredNetworkWordCount")
      .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<String> lines = spark
      .readStream()
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load().as(Encoders.STRING());

    // Split the lines into words
    Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
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