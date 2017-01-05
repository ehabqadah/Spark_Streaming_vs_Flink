package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class DateTimeUtils {

	/**
	 * Parse date time based on format (e.g yyyy/MM/dd HH:mm:ss)
	 * 
	 * @param dateTimeStr
	 * @return
	 */
	public static LocalDateTime parseDateTime(String dateTimeStr, String format) {

		LocalDateTime date = null;
		try {

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
			date = LocalDateTime.parse(dateTimeStr, formatter);
		} catch (DateTimeParseException exc) {
			System.out.printf("%s is not parsable!%n %s", dateTimeStr, exc);
		}

		return date;

	}
}
