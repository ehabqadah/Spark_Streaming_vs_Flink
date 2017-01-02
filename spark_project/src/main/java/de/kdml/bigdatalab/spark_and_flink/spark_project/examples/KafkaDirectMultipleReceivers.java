package de.kdml.bigdatalab.spark_and_flink.spark_project.examples;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import de.kdml.bigdatalab.spark_and_flink.spark_project.Configs;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
public class KafkaDirectMultipleReceivers {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		// Create context with a 3 seconds batch interval
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Java DirectKafkaWordCount with update state by key");

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(configs.getStringProp("topics").split(",")));
		
		

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", configs.getStringProp("brokers"));
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", configs.getStringProp("kafkaGroupId"));
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);


		//create two re
		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				jssc,
			    LocationStrategies.PreferConsistent(),
			    ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
			  );

		JavaInputDStream<ConsumerRecord<String, String>> stream1 = KafkaUtils.createDirectStream(
				jssc,
			    LocationStrategies.PreferConsistent(),
			    ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
			  );
		

		 JavaPairDStream<String, String> messages = stream.mapToPair(
				  new PairFunction<ConsumerRecord<String, String>, String, String>() {
				    @Override
				    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				      return new Tuple2<>(record.key(), record.value());
				    }
				  });
		 JavaPairDStream<String, String> messages1 = stream1.mapToPair(
				  new PairFunction<ConsumerRecord<String, String>, String, String>() {
				    @Override
				    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				      return new Tuple2<>(record.key(), record.value());
				    }
				  });
		 
		 
		messages.foreachRDD((rdd,time) -> {

			System.out.println(time+"stream data:" + rdd.collect());
		});
		
		messages1.foreachRDD((rdd,time) -> {

			System.out.println(time+"stream1 data:" + rdd.collect());
		});
		

		JavaPairDStream<String, String> unionStream = messages.union(messages1);
		
		unionStream.print();
		
		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("error:" + e.getMessage());
		}
	}
}
