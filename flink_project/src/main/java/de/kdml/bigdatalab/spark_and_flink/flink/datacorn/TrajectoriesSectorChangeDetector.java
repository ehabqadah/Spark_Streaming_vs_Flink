package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
import de.kdml.bigdatalab.spark_and_flink.flink.utils.FlinkUtils;

/**
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class TrajectoriesSectorChangeDetector {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = FlinkUtils.getInitializedEnv();
		KeyedStream<Tuple2<String, PositionMessage>, Tuple> trajectoriesStream = TrajectoriesStreamUtils
				.getTrajectoriesStream(env);

		DataSet<Sector> sectorsDataSet = getSectorDataSet();

		// assign sector for each trajectory, then check if there is change in
		// the sector between two consecutive trajectories
		DataStream<String> trajectoriesStreamWithSectors = trajectoriesStream
				.reduce(new TrajectoriesSectorsReducer(sectorsDataSet.collect())).filter(tuple -> {
					PositionMessage position = tuple.f1;
					// check for change in previous and current sector
					if (position.getPrevSector() != null && !position.getSector().equals(position.getPrevSector())) {
						return true;
					}
					return false;
				}).map(tuple -> {

					// print the sector transition
					PositionMessage trajectory = tuple.f1;
					return tuple.f0 + ": " + trajectory.getPrevSector().getNameAndAirBlock() + " --> "
							+ trajectory.getSector().getNameAndAirBlock();

				});

		trajectoriesStreamWithSectors.print().setParallelism(1);
		env.execute("Trajectories Sector change detector");
	}

	/***
	 * Get the sectors data set
	 */
	private static DataSet<Sector> getSectorDataSet() {
		// get the sectors list
		ExecutionEnvironment dataSetAPIEnv = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Sector> sectorsDataSet = dataSetAPIEnv.readTextFile(configs.getStringProp("sectorsFilePath"))
				.setParallelism(1).map(line -> {
					return Sector.parseSectorData(line);
				}).setParallelism(1);
		return sectorsDataSet;
	}
}
