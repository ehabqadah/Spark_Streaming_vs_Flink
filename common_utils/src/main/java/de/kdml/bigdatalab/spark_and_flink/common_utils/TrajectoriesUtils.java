package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

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
	 * Sort the positions based on created time
	 * 
	 * @param positions
	 * @return
	 */
	public static List<PositionMessage> sortPositionsOfTrajectory(List<PositionMessage> positions) {

		Iterator<PositionMessage> sortedTrajectories = positions.stream().sorted((trajectory1, trajector2) -> {

			return -1 * trajectory1.getCreatedDateTime().compareTo(trajector2.getCreatedDateTime());
		}).iterator();

		return Lists.newArrayList(sortedTrajectories);

	}

	/**
	 * Calculate Speed and distance of trajectory
	 * 
	 * @param prevPosition
	 * @param currentPosition
	 */
	public static void calculateDistanceAndSpeedOfTrajectory(PositionMessage prevPosition,
			PositionMessage currentPosition) {

		if (currentPosition.getSpeed() != null && currentPosition.getDistance() != null) {
			return;
		}
		double distance = prevPosition == null ? 0
				: GeoUtils.greatCircleDistance(prevPosition.getLatitude(), prevPosition.getLongitude(),
						currentPosition.getLatitude(), currentPosition.getLongitude()),
				speed = 0.0, diffTime, acceleration = 0.0;

		if (prevPosition != null) {

			long diff = Duration.between(prevPosition.getCreatedDateTime(), currentPosition.getCreatedDateTime())
					.toMillis();
			diffTime = ((double) (diff)) / (1000.0 * 60.0 * 60.0);

			if (diffTime != 0.0) {
				speed = distance / diffTime;
				acceleration = (speed - prevPosition.getSpeed()) / diffTime;
			}

		}

		currentPosition.setSpeed(speed);
		currentPosition.setDistance(distance);
		currentPosition.setAcceleration(acceleration);

	}
}
