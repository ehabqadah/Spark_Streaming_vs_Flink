package de.kdml.bigdatalab.spark;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

/***
 * Word counts utils 
 * 
 * @author ehab
 *
 */
public class WordCountsUtil {

	/**
	 * Get  list of files path under the directory  recursively 
	 * @param dirPath
	 * @return
	 */
	public static List<String> getAllFiles(String dirPath) {

		List<String> files = new ArrayList<>();
		File folder = new File(dirPath);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			File file = listOfFiles[i];
			String filePath = file.getPath();
			if (file.isFile()) {

				if (!file.isHidden() && !file.getName().startsWith("_"))
					files.add(filePath);
			} else if (file.isDirectory()) {

				files.addAll(getAllFiles(filePath));
			}
		}

		return files;
	}
	
	/**
	 * Append the new word counts and print the aggregation result
	 * 
	 * @param sc
	 * @param newWordCounts
	 * @param outputDir
	 */
	public static void aggregateWordCountsAndPrint(JavaSparkContext sc ,JavaPairDStream<String, Integer> newWordCounts,String outputDir){
		
		// Apply the inner function to each RDD in this newWordCounts DStream
		newWordCounts.foreachRDD((rdd, time) -> {

	
			if (rdd.count()==0)
			{
				  System.out.println("No words in this time interval: " +time);
				return;
			}
			JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
			result.saveAsHadoopFile(outputDir+"/" + time.toString(), Text.class, IntWritable.class,
					SequenceFileOutputFormat.class);
			//Print the aggregation word counts 
			printAllWordCounts(sc, outputDir);

		});
		
		
		
	}


	/**
	 * Print the aggregation word counts 
	 * 
	 * @param sc
	 * @param outputDir
	 */
	private static void printAllWordCounts(JavaSparkContext sc, String outputDir) {
		List<String> files = WordCountsUtil.getAllFiles(outputDir);
		JavaPairRDD<String, Integer> totalPairs= null;
		for (String file : files) {

			if (new File(file).exists()) {
				JavaPairRDD<Text, IntWritable> input = sc.sequenceFile(file, Text.class, IntWritable.class);
				JavaPairRDD<String, Integer> result2 = input.mapToPair(new ConvertToNativeTypes());
			
				if(totalPairs==null)
				{
					totalPairs=result2;
				}
				else{
					totalPairs=result2.union(totalPairs);
				}
				
			}
		}
		
		totalPairs= totalPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			
			private static final long serialVersionUID = -7143103360793492209L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1+v2;
			}
		});
		
		
		
//			totalPairs.foreach( new VoidFunction<Tuple2<String,Integer>>() {
//				
//				@Override
//				public void call(Tuple2<String, Integer> t) throws Exception {
//					// TODO Auto-generated method stub
//					System.out.println(t);
//				}
//			});
		
		System.out.println(totalPairs.collect());
	}
	
	
	public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
	
		private static final long serialVersionUID = 2433095216631597709L;

		public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
			return new Tuple2(new Text(record._1), new IntWritable(record._2));
		}
	}

	public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {
		
		private static final long serialVersionUID = 476263154663174520L;

		public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) {
			return new Tuple2(record._1.toString(), record._2.get());
		}
	}
}
