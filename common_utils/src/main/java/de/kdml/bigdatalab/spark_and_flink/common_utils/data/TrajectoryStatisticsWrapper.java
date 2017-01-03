package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.io.Serializable;

/**
 *  Trajectory statistics wrapper 
 *  
 * @author Ehab Qadah 
 * 
 * Jan 3, 2017
 */
public class TrajectoryStatisticsWrapper implements Serializable {



	public static final long serialVersionUID = 686131645808716203L;

	private long numPoints;

	private double minSpeed;

	private double maxSpeed;

	private double avgSpeed;

	private double medianSpeed;

	private double minAcceleration;

	private double maxAcceleration;

	private double avgAcceleration;

	private double medianAcceleration;

	private double minDifftime;

	private double maxDifftime;

	private double avgDifftime;

	private double median_difftime;

	private double minDate;
	
	private double maxDate;

	private double minLong;

	private double maxLong;
	
	private double minLat;

	private double maxLat;

	

	public TrajectoryStatisticsWrapper() {
	}

	public TrajectoryStatisticsWrapper(//
			String id,//
			int id_c, //
			long nr_points, //
			double min_speed, //
			double max_speed, //
			double avg_speed, //
			double median_speed, //
			double min_acceleration, //
			double max_acceleration, //
			double avg_acceleration, //
			double median_acceleration, //
			double min_difftime, //
			double max_difftime, //			
			double avg_difftime, //
			double median_difftime, //
			double min_date, //
			double max_date, //
			double min_X, //
			double max_X, //			
			double min_Y, //
			double max_Y) {
		
		super();
		
		
		this.setNumPoints(nr_points);
		this.setMinSpeed(min_speed);
		this.setMaxSpeed(max_speed);
		this.setAvgSpeed(avg_speed);
		this.setMedianSpeed(median_speed);
		this.setMinAcceleration(min_acceleration);
		this.setMaxAcceleration(max_acceleration);
		this.setAvgAcceleration(avg_acceleration);
		this.setMedianAcceleration(median_acceleration);
		this.setMinDifftime(min_difftime);
		this.setMaxDifftime(max_difftime);
		this.setAvgDifftime(avg_difftime);
		this.setMedian_difftime(median_difftime);
		this.setMinDate(min_date);
		this.setMaxDate(max_date);
		this.setMinLong(min_X);
		this.setMaxLong(max_X);
		this.setMinLat(min_Y);
		this.setMaxLat(max_Y);
	}

	
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Statistics ");
	
		builder.append("[ nr_points=");
		builder.append(getNumPoints());
		builder.append(", min_speed=");
		builder.append(getMinSpeed());
		builder.append(", max_speed=");
		builder.append(getMaxSpeed());
		builder.append(", avg_speed=");
		builder.append(getAvgSpeed());
		builder.append(", median_speed=");
		builder.append(getMedianSpeed());
		builder.append(", min_acceleration=");
		builder.append(getMinAcceleration());
		builder.append(", max_acceleration=");
		builder.append(getMaxAcceleration());
		builder.append(", avg_acceleration=");
		builder.append(getAvgAcceleration());
		builder.append(", median_acceleration=");
		builder.append(getMedianAcceleration());
		builder.append(", min_difftime=");
		builder.append(getMinDifftime());
		builder.append(", max_difftime=");
		builder.append(getMaxDifftime());
		builder.append(", avg_difftime=");
		builder.append(getAvgDifftime());
		builder.append(", median_difftime=");
		builder.append(getMedian_difftime());
		builder.append(", min_date=");
		builder.append(getMinDate());
		builder.append(", max_date=");
		builder.append(getMaxDate());
		builder.append(", min_X=");
		builder.append(getMinLong());
		builder.append(", max_X=");
		builder.append(getMaxLong());
		builder.append(", min_Y=");
		builder.append(getMinLat());
		builder.append(", max_Y=");
		builder.append(getMaxLat());
		builder.append("]");
		return builder.toString();
	}

	public long getNumPoints() {
		return numPoints;
	}

	public void setNumPoints(long numPoints) {
		this.numPoints = numPoints;
	}

	public double getMinSpeed() {
		return minSpeed;
	}

	public void setMinSpeed(double minSpeed) {
		this.minSpeed = minSpeed;
	}

	public double getMaxSpeed() {
		return maxSpeed;
	}

	public void setMaxSpeed(double maxSpeed) {
		this.maxSpeed = maxSpeed;
	}

	public double getAvgSpeed() {
		return avgSpeed;
	}

	public void setAvgSpeed(double avgSpeed) {
		this.avgSpeed = avgSpeed;
	}

	public double getMedianSpeed() {
		return medianSpeed;
	}

	public void setMedianSpeed(double medianSpeed) {
		this.medianSpeed = medianSpeed;
	}

	public double getMinAcceleration() {
		return minAcceleration;
	}

	public void setMinAcceleration(double minAcceleration) {
		this.minAcceleration = minAcceleration;
	}

	public double getMaxAcceleration() {
		return maxAcceleration;
	}

	public void setMaxAcceleration(double maxAcceleration) {
		this.maxAcceleration = maxAcceleration;
	}

	public double getAvgAcceleration() {
		return avgAcceleration;
	}

	public void setAvgAcceleration(double avgAcceleration) {
		this.avgAcceleration = avgAcceleration;
	}

	public double getMedianAcceleration() {
		return medianAcceleration;
	}

	public void setMedianAcceleration(double medianAcceleration) {
		this.medianAcceleration = medianAcceleration;
	}

	public double getMinDifftime() {
		return minDifftime;
	}

	public void setMinDifftime(double minDifftime) {
		this.minDifftime = minDifftime;
	}

	public double getMaxDifftime() {
		return maxDifftime;
	}

	public void setMaxDifftime(double maxDifftime) {
		this.maxDifftime = maxDifftime;
	}

	public double getAvgDifftime() {
		return avgDifftime;
	}

	public void setAvgDifftime(double avgDifftime) {
		this.avgDifftime = avgDifftime;
	}

	public double getMedian_difftime() {
		return median_difftime;
	}

	public void setMedian_difftime(double median_difftime) {
		this.median_difftime = median_difftime;
	}

	public double getMinDate() {
		return minDate;
	}

	public void setMinDate(double minDate) {
		this.minDate = minDate;
	}

	public double getMaxDate() {
		return maxDate;
	}

	public void setMaxDate(double maxDate) {
		this.maxDate = maxDate;
	}

	public double getMinLong() {
		return minLong;
	}

	public void setMinLong(double minLong) {
		this.minLong = minLong;
	}

	public double getMaxLong() {
		return maxLong;
	}

	public void setMaxLong(double maxLong) {
		this.maxLong = maxLong;
	}

	public double getMinLat() {
		return minLat;
	}

	public void setMinLat(double minLat) {
		this.minLat = minLat;
	}

	public double getMaxLat() {
		return maxLat;
	}

	public void setMaxLat(double maxLat) {
		this.maxLat = maxLat;
	}

}
