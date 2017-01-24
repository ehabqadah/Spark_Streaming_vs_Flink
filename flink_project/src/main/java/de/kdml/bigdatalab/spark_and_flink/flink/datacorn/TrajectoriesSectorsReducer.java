package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import de.kdml.bigdatalab.spark_and_flink.common_utils.SectorUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;

public class TrajectoriesSectorsReducer implements ReduceFunction<Tuple2<String, Trajectory>> {

	private static final long serialVersionUID = 644511065257204161L;
	public static List<Sector> sectors = null;

	public TrajectoriesSectorsReducer(List<Sector> sectorsList) {

		sectors = sectorsList;
	}

	@Override
	public Tuple2<String, Trajectory> reduce(Tuple2<String, Trajectory> tuple1, Tuple2<String, Trajectory> tuple2) {

		Tuple2<String, Trajectory> oldTuple = tuple2.f1.isNew() ? tuple1 : tuple2;
		Tuple2<String, Trajectory> newTuple = tuple2.f1.isNew() ? tuple2 : tuple1;

		assignSector(oldTuple.f1);
		assignSector(newTuple.f1);

		newTuple.f1.setPrevSector(oldTuple.f1.getSector());

		newTuple.f1.setNew(false);

		return newTuple;
	}

	private void assignSector(Trajectory trajectory) {
		if (trajectory.getSector() == null) {
			Sector sector = SectorUtils.getSectorForTrajectory(trajectory, sectors);
			trajectory.setSector(sector);
		}
	}
}