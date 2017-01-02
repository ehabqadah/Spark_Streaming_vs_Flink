package de.kdml.bigdatalab.spark_and_flink.spark_project;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;

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
		sc.setCheckpointDir(configs.getStringProp("spark_checkpoint_dir"));

		return sc;
	}
}
