package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
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
		DataStream<Tuple2<String, List<Trajectory>>> trajectoriesStream = TrajectoriesStreamUtils
				.getTrajectoriesStream(env);

		DataSet<Sector> sectorsDataSet = getSectorDataSet();

		// assign sector for each trajectory, then check if there is change in
		// the sector between two consecutive trajectories
		DataStream<String> trajectoriesStreamWithSectors = trajectoriesStream
				.map(new TrajectorySectorMapper(sectorsDataSet.collect())).filter(tuple -> {
					// sort trajectories
					List<Trajectory> trajectories = tuple.f1;
					if (trajectories != null && trajectories.size() > 1) {

						// check for change between trajectories with respect to
						// sectors
						int i = trajectories.size() - 1;
						Trajectory trajectoryi = trajectories.get(i - 1);
						Trajectory trajectoryi1 = trajectories.get(i);

						if ((!trajectoryi.getSector().equals(trajectoryi1.getSector()))) {

							return true;
						}

					}
					return false;
				}).map(tuple -> {

					// print the sector transition
					List<Trajectory> trajectories = tuple.f1;
					int i = trajectories.size() - 1;
					Trajectory trajectoryi = trajectories.get(i - 1);
					Trajectory trajectoryi1 = trajectories.get(i);
					return tuple.f0 + ": " + trajectoryi.getSector().getNameAndAirBlock() + " --> "
							+ trajectoryi1.getSector().getNameAndAirBlock();

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
