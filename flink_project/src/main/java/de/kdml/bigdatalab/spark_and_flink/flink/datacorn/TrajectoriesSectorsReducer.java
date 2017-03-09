package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import de.kdml.bigdatalab.spark_and_flink.common_utils.LoggerUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.SectorUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;

/**
 * This reducer assign previous and current sector for each new tuple of
 * (id,position message) and discard the old tuple and keeps the new one
 * 
 * @author Ehab Qadah
 * 
 *         Mar 3, 2017
 */

public class TrajectoriesSectorsReducer implements ReduceFunction<Tuple2<String, PositionMessage>> {

	private static final long serialVersionUID = 644511065257204161L;
	public static List<Sector> sectors = null;

	public TrajectoriesSectorsReducer(List<Sector> sectorsList) {

		sectors = sectorsList;
	}

	@Override
	public Tuple2<String, PositionMessage> reduce(Tuple2<String, PositionMessage> tuple1,
			Tuple2<String, PositionMessage> tuple2) {

		Tuple2<String, PositionMessage> oldTuple = tuple2.f1.isNew() ? tuple1 : tuple2;
		Tuple2<String, PositionMessage> newTuple = tuple2.f1.isNew() ? tuple2 : tuple1;

		assignSector(oldTuple.f1);
		assignSector(newTuple.f1);

		newTuple.f1.setPrevSector(oldTuple.f1.getSector());

		newTuple.f1.setNew(false);

		LoggerUtils.logMessage("-" + System.currentTimeMillis());
		LoggerUtils.logMessage("-" + System.currentTimeMillis());
		return newTuple;
	}

	private void assignSector(PositionMessage trajectory) {
		if (trajectory.getSector() == null) {
			Sector sector = SectorUtils.getSectorForTrajectory(trajectory, sectors);
			trajectory.setSector(sector);
		}
	}
}