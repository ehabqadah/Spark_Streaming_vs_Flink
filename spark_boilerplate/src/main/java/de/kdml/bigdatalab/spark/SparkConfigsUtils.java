package de.kdml.bigdatalab.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This to general spark configs and context utils
 * 
 * @author ehab
 *
 */
public class SparkConfigsUtils {

	private static Configs configs = Configs.getInstance();

	/**
	 * Get initialized spark context instance
	 * 
	 * @param appName
	 */
	public static JavaSparkContext getSparkContext(String appName) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Logger.getRootLogger().setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf().setMaster(configs.getStringProp("spark_master")).setAppName(appName);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("OFF");

		return sc;
	}
}
