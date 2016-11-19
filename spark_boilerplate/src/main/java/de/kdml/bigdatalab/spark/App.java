package de.kdml.bigdatalab.spark;

import org.apache.spark.api.java.*;

import java.util.HashMap;
import java.util.HashSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;

public class App {
	
	
	public static final  AtomicReference<JavaPairDStream<String, Integer>> wordsStream = new AtomicReference<JavaPairDStream<String,Integer>>(null);
	
	
	// Stats will be computed for the last window length of time.
	  private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
	  // Stats will be computed every slide interval time.
	  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);
	public static void main(String[] args) {


		  
	    String brokers = "localhost:9092";
	    String topics = "test";

	    // Create context with a 2 seconds batch interval
	    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

	    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    Map<String, String> kafkaParams = new HashMap<>();
	    kafkaParams.put("metadata.broker.list", brokers);

	    // Create direct kafka stream with brokers and topics
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        String.class,
	        StringDecoder.class,
	        StringDecoder.class,
	        kafkaParams,
	        topicsSet
	    );
	    // Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	      @Override
	      public String call(Tuple2<String, String> tuple2) {
	        return tuple2._2();
	      }
	    });
	    
	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      @Override
	      public Iterator<String> call(String x) {
	        return Arrays.asList(x.split(" ")).iterator();
	      }
	    }).persist();
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<>(s, 1);
	        }
	      }).reduceByKey(
	        new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      });
	    
	    JavaDStream<String> windowDStream =
	    		words.window(WINDOW_LENGTH, SLIDE_INTERVAL);
	    
	    windowDStream.foreachRDD(word -> {
	    	
	    	System.out.println("count ="+word.count());
	    });
//	    if(wordsStream.get() ==null){
//	    	System.out.println("\n \n \n ---------------------vvvvvvv------");
//	    	wordCounts.print();
//	    	wordsStream.set(wordCounts);
//	    }
//	    else{
//	    	wordsStream.updateAndGet(n -> n.union(wordCounts));
//	    	
//	    	System.out.println("\n \n \n xxxx---------------------vvvvvvv------");
//	    	wordsStream.get().print();
//	       	System.out.println("\n \n \n ---------------------vvvvv-----");
//	    }
	  
	    
//	    wordCounts.foreachRDD(rdd -> {
//	        List<Tuple2<String, Integer>> ipAddresses = rdd.take(100);
//	        System.out.println("All IPAddresses > 10 times: " + ipAddresses);
//	      });
	    // Start the computation
	    jssc.start();
	    try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		SparkConf conf = new SparkConf().setAppName("Spark Boilerplate App");
//		
//		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(2));
//		 int numThreads = 1;
//		    Map<String, Integer> topicMap = new HashMap<>();
//		    String[] topics = {"test"};
//		    for (String topic: topics) {
//		      topicMap.put(topic, numThreads);
//		    }
//
//		    JavaPairReceiverInputDStream<String, String> messages =
//		            KafkaUtils.createStream(jssc, "localhost:2181", "mygroup", topicMap);
//		 
//	 // Get the lines, split them into words, count the words and print
//	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//	      @Override
//	      public String call(Tuple2<String, String> tuple2) {
//	        return tuple2._2();
//	      }
//	    });
//	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//	      @Override
//	      public Iterator<String> call(String x) {
//	        return Arrays.asList(x.split(" ")).iterator();
//	      }
//	    });
//	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//	      new PairFunction<String, String, Integer>() {
//	        @Override
//	        public Tuple2<String, Integer> call(String s) {
//	          return new Tuple2<>(s, 1);
//	        }
//	      }).reduceByKey(
//	        new Function2<Integer, Integer, Integer>() {
//	        @Override
//	        public Integer call(Integer i1, Integer i2) {
//	          return i1 + i2;
//	        }
//	      });
//	    wordCounts.print();
//
//	    // Start the computation
//	    jssc.start();
//	    try {
//			jssc.awaitTermination();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	    
		/*JavaReceiverInputDStream<String> lines= jssc.socketTextStream("localhost", 9999,StorageLevels.MEMORY_AND_DISK_SER);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String x) {
		        return Arrays.asList(x.split(" ")).iterator();
		      }
		    });
		    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
		      new PairFunction<String, String, Integer>() {
		        @Override
		        public Tuple2<String, Integer> call(String s) {
		          return new Tuple2<>(s, 1);
		        }
		      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
		        @Override
		        public Integer call(Integer i1, Integer i2) {
		          return i1 + i2;
		        }
		      });

		    if(wordsStream ==null){
		    	wordsStream=wordCounts;
		    	wordsStream.persist();
		    }
		    else{
		    	wordsStream= wordsStream.union(wordCounts);
		    	System.out.println("\n \n \n ---------------------vvvvvvv------");
		    	wordsStream.print();
		       	System.out.println("\n \n \n ---------------------vvvvv-----");
		    }
		    wordCounts.print();
		    jssc.start();
		    try {
				jssc.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		
		
		/*JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> linesRDD = sc.textFile("/usr/local/spark/spark-2.0.1-bin-hadoop2.7/README.md").cache();

		JavaRDD<String> words = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String s) {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		System.out.println("---------------------------------------\n \n " + counts.take(50)
				+ "\n \n ----------------------------------");

		sc.close();*/
	}
}
