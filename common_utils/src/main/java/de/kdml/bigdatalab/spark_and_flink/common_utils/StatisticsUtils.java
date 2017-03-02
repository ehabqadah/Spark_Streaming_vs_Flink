package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.util.List;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.TrajectoryStatisticsWrapper;

/**
 * 
 * 
 * @author Ehab Qadah
 * 
 *         Jan 3, 2017
 */
public class StatisticsUtils {

	/**
	 * 
	 * @param trajectories
	 * @return
	 */
	public static List<PositionMessage> computeStatistics(List<PositionMessage> trajectories) {

		double minLongtitude = Double.MAX_VALUE, minLatitude = Double.MAX_VALUE, minAltitude = Double.MAX_VALUE,
				minSpeed = Double.MAX_VALUE, minAcceleration = Double.MAX_VALUE;
		double maxLongtitude = Double.MIN_VALUE, maxLatitude = Double.MIN_VALUE, maxAltitude = Double.MIN_VALUE,
				maxSpeed = Double.MIN_VALUE, maxAcceleration = Double.MIN_VALUE;

		PositionMessage prevTrajectory = null;
		for (PositionMessage trajectory : trajectories) {

			TrajectoriesUtils.calculateDistanceAndSpeedOfTrajectory(prevTrajectory, trajectory);

			double longtitude = trajectory.getLongitude(), lat = trajectory.getLatitude(),
					altit = trajectory.getAltitude(), speed = trajectory.getSpeed(),
					acceleration = trajectory.getAcceleration();
			// update min longitude
			minLongtitude = Math.min(minLongtitude, longtitude);
			// update min latitude
			minLatitude = Math.min(minLatitude, lat);
			minAltitude = Math.min(minAltitude, altit);
			minAcceleration = Math.min(minAcceleration, acceleration);
			minSpeed = Math.min(minSpeed, speed);
			// update max longitude
			maxLongtitude = Math.max(maxLongtitude, longtitude);
			// update max latitude
			maxLatitude = Math.max(maxLatitude, lat);
			maxAltitude = Math.max(maxAltitude, altit);
			maxSpeed = Math.max(maxSpeed, speed);
			maxAcceleration = Math.max(maxAcceleration, acceleration);

			prevTrajectory = trajectory;

		}

		TrajectoryStatisticsWrapper statistics = new TrajectoryStatisticsWrapper();
		statistics.setMinLong(minLongtitude);
		statistics.setMinLat(minLatitude);
		statistics.setMinAltitude(minAltitude);
		statistics.setMaxLong(maxLongtitude);
		statistics.setMaxLat(maxLatitude);
		statistics.setMaxAltitude(maxAltitude);
		statistics.setMaxAcceleration(maxAcceleration);
		statistics.setMinAcceleration(minAcceleration);
		statistics.setMaxSpeed(maxSpeed);
		statistics.setMinSpeed(minSpeed);

		for (PositionMessage trajectory : trajectories) {

			// just update new trajectories
			if (trajectory.getStatistics() == null) {

				trajectory.setStatistics(statistics);
			}

		}

		return trajectories;
	}

	/**
	 * Compute statistics by aggregating from old trajectory
	 * 
	 * @param oldPosition
	 * @param newPosition
	 */
	public static void computeStatistics(PositionMessage oldPosition, PositionMessage newPosition) {

		if (oldPosition == null) {
			intitStaticsForIntitialPositionOfTrajectory(newPosition);
			return;
		}
		intitStaticsForIntitialPositionOfTrajectory(oldPosition);// initialize
																	// statics
																	// for
		// the first trajectory only
		double minLongtitude, minLatitude, minAltitude, minSpeed, minAcceleration;
		double maxLongtitude, maxLatitude, maxAltitude, maxSpeed, maxAcceleration;
		TrajectoriesUtils.calculateDistanceAndSpeedOfTrajectory(oldPosition, newPosition);
		double longtitude = newPosition.getLongitude(), lat = newPosition.getLatitude(),
				altit = newPosition.getAltitude(), speed = newPosition.getSpeed(),
				acceleration = newPosition.getAcceleration(), newSpeed = newPosition.getSpeed();
		TrajectoryStatisticsWrapper oldStatistics = oldPosition.getStatistics();
		// update min longitude
		minLongtitude = Math.min(oldStatistics.getMinLong(), longtitude);
		// update min latitude
		minLatitude = Math.min(oldStatistics.getMinLat(), lat);
		minAltitude = Math.min(oldStatistics.getMinAltitude(), altit);
		minAcceleration = Math.min(oldStatistics.getMaxAcceleration(), acceleration);
		minSpeed = Math.min(oldStatistics.getMinSpeed(), speed);
		// update max longitude
		maxLongtitude = Math.max(oldStatistics.getMaxLong(), longtitude);
		// update max latitude
		maxLatitude = Math.max(oldStatistics.getMaxLat(), lat);
		maxAltitude = Math.max(oldStatistics.getMaxAltitude(), altit);
		maxSpeed = Math.max(oldStatistics.getMaxSpeed(), speed);
		maxAcceleration = Math.max(oldStatistics.getMaxAcceleration(), acceleration);

		// compute aggergate speed sum & number of points to find avg speed
		double speedSum = newSpeed + oldStatistics.getAggergatedSpeedSum();
		long numberOfpoints = oldStatistics.getNumPoints() + 1;
		double avgSpeed = speedSum / (double) numberOfpoints;
		TrajectoryStatisticsWrapper statistics = new TrajectoryStatisticsWrapper();
		statistics.setMinLong(minLongtitude);
		statistics.setMinLat(minLatitude);
		statistics.setMinAltitude(minAltitude);
		statistics.setMaxLong(maxLongtitude);
		statistics.setMaxLat(maxLatitude);
		statistics.setMaxAltitude(maxAltitude);
		statistics.setMaxAcceleration(maxAcceleration);
		statistics.setMinAcceleration(minAcceleration);
		statistics.setMaxSpeed(maxSpeed);
		statistics.setMinSpeed(minSpeed);
		statistics.setNumPoints(numberOfpoints);
		statistics.setAggergatedSpeedSum(speedSum);
		statistics.setAvgSpeed(avgSpeed);
		newPosition.setStatistics(statistics);
	}

	/**
	 * Initialize the statistics of the first postition in the trajectory
	 * 
	 * @param oldPosition
	 */
	private static void intitStaticsForIntitialPositionOfTrajectory(PositionMessage oldPosition) {
		if (oldPosition.isNew() || oldPosition.getStatistics() == null) {

			TrajectoryStatisticsWrapper statistics = new TrajectoryStatisticsWrapper();
			statistics.setMinLong(oldPosition.getLongitude());
			statistics.setMinLat(oldPosition.getLatitude());
			statistics.setMinAltitude(oldPosition.getAltitude());
			statistics.setMaxLong(oldPosition.getLongitude());
			statistics.setMaxLat(oldPosition.getLatitude());
			statistics.setMaxAltitude(oldPosition.getAltitude());
			statistics.setMaxAcceleration(0);
			statistics.setMinAcceleration(0);
			statistics.setMaxSpeed(0);
			statistics.setMinSpeed(0);
			statistics.setAvgSpeed(0);
			statistics.setNumPoints(1);
			oldPosition.setStatistics(statistics);
			oldPosition.setSpeed(0.0);
			oldPosition.setAcceleration(0.0);
			oldPosition.setDistance(0.0);
		}

	}

}
