package de.kdml.bigdatalab.spark;


import org.apache.spark.api.java.*;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class App {
  public static void main(String[] args) {
	  
	  
    SparkConf conf = new SparkConf().setAppName("Spark Boilerplate App");
    
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    JavaRDD<String> linesRDD = sc.textFile("/usr/local/spark/spark-2.0.1-bin-hadoop2.7/README.md").cache();

    JavaRDD<String> words = linesRDD.flatMap(new FlatMapFunction<String, String>() {
    	  public Iterator<String> call(String s) { return Arrays.asList(s.split(" ")).iterator(); }
    	});
    	JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
    	  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
    	});
    	JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
    	  public Integer call(Integer a, Integer b) { return a + b; }
    	});
    	
    	 System.out.println("---------------------------------------\n \n "+counts.take(50)+"\n \n ----------------------------------");
    	 
    	 
    	 sc.close();
  }
}



