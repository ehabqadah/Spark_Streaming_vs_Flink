package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.util.List;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;

/**
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class SectorUtils {

	/**
	 * Assign the container sector for trajectory
	 * 
	 * @param trajectory
	 * @param sectors
	 * @return
	 */
	public static Sector getSectorForTrajectory(Trajectory trajectory, List<Sector> sectors) {

		for (Sector sector : sectors) {

			if (GeoUtils.isPointInPolygon(sector.getPolygon(), trajectory.getLongtitude(), trajectory.getLatitude())) {
				return sector;

			}
		}
		return null;

	}
}