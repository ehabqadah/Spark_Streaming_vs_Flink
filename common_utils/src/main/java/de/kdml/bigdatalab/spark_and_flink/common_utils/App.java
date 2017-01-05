package de.kdml.bigdatalab.spark_and_flink.common_utils;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.DateTimeUtils;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args)
    {
        System.out.println( "Hello World!" );
        
        DateTimeUtils.parseDateTime("2015/08/13 15:41:37.181","yyyy/MM/dd HH:mm:ss.SSS");
    }
}
