package de.kdml.bigdatalab.spark_and_flink.common_utils;

public class Utils {

	/**
	 * Get double value from string ot get default value
	 * 
	 * @param s
	 * @param defaultValue
	 * @return
	 */
	public static double parseDoubleOrElse(String s, double defaultValue) {

		try {
			return Double.parseDouble(s);
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
