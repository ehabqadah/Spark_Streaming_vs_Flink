package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;

/***
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class TrajectoriesUtils {

	private static final String DATE_TIME_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";

	/**
	 * Generate a trajectory from input data string in format of
	 * (MSG,3,,,34324E,,2015/08/03,01:05:03.844,2015/08/03,01:05:07.058,,30050,,,45.69032,5.54741,,,0,0,0,0)
	 * 
	 * @param line
	 * @return
	 */
	public static Trajectory parseDataInput(String line) {

		String[] attributes = line.split(",");

		Trajectory trajectory = new Trajectory();
		if (attributes.length > 5) {
			trajectory.setType(attributes[0] + attributes[1]);
			trajectory.setID(attributes[4]);
			trajectory.setCreatedDateTime(
					DateTimeUtils.parseDateTime(attributes[6] + " " + attributes[7], DATE_TIME_FORMAT));

		}

		if (attributes.length > 11) {
			// get location coordinates
			trajectory.setAltitude(Utils.parseDoubleOrElse(attributes[11], 0.0));
			trajectory.setLongtitude(Utils.parseDoubleOrElse(attributes[15], 0.0));
			trajectory.setLatitude(Utils.parseDoubleOrElse(attributes[14], 0.0));
		}
		return trajectory;

	}

	/**
	 * Sort the trajectories based on created time
	 * 
	 * @param trajectories
	 * @return
	 */
	public static List<Trajectory> sortTrajectories(List<Trajectory> trajectories) {

		Iterator<Trajectory> sortedTrajectories = trajectories.stream().sorted((trajectory1, trajector2) -> {

			return trajectory1.getCreatedDateTime().compareTo(trajector2.getCreatedDateTime());
		}).iterator();

		return Lists.newArrayList(sortedTrajectories);

	}
}
