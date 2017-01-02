package de.kdml.bigdatalab.spark.examples;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import de.kdml.bigdatalab.spark.Configs;

import java.util.Arrays;
import java.util.Iterator;

/***
 * Note: not working yet 
 * @author ehab
 *
 */
public final class StructuredKafkaWordCount {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		SparkSession spark = SparkSession.builder().master(configs.getStringProp("spark_master")).appName("StructuredKafkaWordCount").getOrCreate();

		// Create DataSet representing the stream of input lines from kafka
		Dataset<String> lines = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", configs.getStringProp("brokers"))
				.option("key.deserializerr", "org.apache.kafka.common.serialization.ByteArraySerializer")
				.option("key.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
				.option("subscribe", configs.getStringProp("topics")).load().selectExpr("CAST(value AS STRING)")
				.as(Encoders.STRING());

		// Generate running word count
		Dataset<Row> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		}, Encoders.STRING()).groupBy("value").count();

		// Start running the query that prints the running counts to the console
		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

		query.awaitTermination();
	}
}
