package de.kdml.bigdatalab.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
public class StructueredStreamingKafaka {
	public static void main(String[] args) throws Exception {

//		SparkSession spark = SparkSession.builder().appName("Structured streaming +kafaka").getOrCreate();
//
//		Dataset<Row> ds1 = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
//				.option("subscribe", "test").load();
//
//		ds1=ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//		
//		
////		Dataset<String> words = ds1.flatMap(new FlatMapFunction<Row, String>() {
////			@Override
////			public Iterator<String> call(Row x) {
////				return Arrays.asList(x.getString(1).split(" ")).iterator();
////			}
////		}, Encoders.STRING());
////		
//		
//
//		// Generate running word count
//		Dataset<Row> wordCounts = ds1.groupBy("value").count();
//		// wordCounts.write().saveAsTable("wordCounts");
//		// Start running the query that prints the running counts to the console
//		StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();
//
//		query.awaitTermination();
		
		
		// Create context with a 2 seconds batch interval
				SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
				JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		

		Collection<String> topics = Arrays.asList("test");

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
				  jssc,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );

//		JavaPairDStream<String, String> lines = stream.mapToPair(
//		  new PairFunction<ConsumerRecord<String, String>, String, String>() {
//		    @Override
//		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
//		      return new Tuple2<>(record.key(), record.value());
//		    }
//		  });
		
		stream.count().print();
		// Start the computation
				jssc.start();
				try {
					jssc.awaitTermination();
				} catch (InterruptedException e) {
					System.out.println("error:" + e.getMessage());
				}

	}

}
