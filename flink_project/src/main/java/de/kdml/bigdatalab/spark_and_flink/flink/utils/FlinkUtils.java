package de.kdml.bigdatalab.spark_and_flink.flink.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Common Flink Utils 
 * 
 * @author Ehab Qadah 
 * 
 * Jan 2, 2017
 */
public class FlinkUtils {

	public static StreamExecutionEnvironment getInitializedEnv(){
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time characteristics
		env.getConfig().disableSysoutLogging();

		// In case of a failure the system tries to restart the job 4 times and
		// waits 10 seconds in-between successive restart attempts.
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// generate a Watermark every second
		env.getConfig().setAutoWatermarkInterval(1000);

		// Enables checkpointing for the streaming job. The distributed state of
		// the streaming dataflow will be periodically snapshotted
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.setParallelism(4);
		return env;
	}
}
