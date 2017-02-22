package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import javax.swing.text.html.HTMLDocument.HTMLReader.SpecialAction;

import com.google.common.collect.Lists;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;

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
	public static PositionMessage parseDataInput(String line) {

		String[] attributes = line.split(",");

		PositionMessage trajectory = new PositionMessage();
		if (attributes.length > 5) {
			trajectory.setType(attributes[0] + attributes[1]);
			trajectory.setID(attributes[4]);
			trajectory.setCreatedDateTime(
					DateTimeUtils.parseDateTime(attributes[6] + " " + attributes[7], DATE_TIME_FORMAT));

		}

		if (attributes.length > 11) {
			// get location coordinates
			trajectory.setAltitude(Utils.parseDoubleOrElse(attributes[11], 0.0));
			trajectory.setLongitude(Utils.parseDoubleOrElse(attributes[15], 0.0));
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
	public static List<PositionMessage> sortTrajectories(List<PositionMessage> trajectories) {

		Iterator<PositionMessage> sortedTrajectories = trajectories.stream().sorted((trajectory1, trajector2) -> {

			return -1 * trajectory1.getCreatedDateTime().compareTo(trajector2.getCreatedDateTime());
		}).iterator();

		return Lists.newArrayList(sortedTrajectories);

	}

	/**
	 * Calculate Speed and distance of trajectory
	 * 
	 * @param prevTrajectory
	 * @param trajectory
	 */
	public static void calculateDistanceAndSpeedOfTrajectory(PositionMessage prevTrajectory, PositionMessage trajectory) {

		if (trajectory.getSpeed() != null && trajectory.getDistance() != null) {
			return;
		}
		double distance = prevTrajectory == null ? 0
				: GeoUtils.greatCircleDistance(prevTrajectory.getLatitude(), prevTrajectory.getLongitude(),
						trajectory.getLatitude(), trajectory.getLongitude()),
				speed = 0.0, diffTime, acceleration = 0.0;

		if (prevTrajectory != null) {

			long diff = Duration.between(prevTrajectory.getCreatedDateTime(), trajectory.getCreatedDateTime())
					.toMillis();
			diffTime = ((double) (diff)) / (1000 * 60 * 60);

			if (diffTime != 0.0) {
				speed = distance / diffTime;
				acceleration = (speed - prevTrajectory.getSpeed()) / diffTime;
			}

		}

		trajectory.setSpeed(speed);
		trajectory.setDistance(distance);
		trajectory.setAcceleration(acceleration);

	}
}
