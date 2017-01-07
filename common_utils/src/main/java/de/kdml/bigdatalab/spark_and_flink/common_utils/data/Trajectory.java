package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.io.Serializable;
import java.time.LocalDateTime;

import de.kdml.bigdatalab.spark_and_flink.common_utils.DateTimeUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.Utils;

public class Trajectory implements Serializable {

	private static final long serialVersionUID = 1013765199466780042L;
	private String ID;
	private double latitude;
	private double longtitude;
	private double altitude;
	private String type;
	private TrajectoryStatisticsWrapper statistics;
	private LocalDateTime createdDateTime;
	private Sector sector;
	private boolean isNew;
	private long streamedTime;

	public Trajectory() {
	}

	public Trajectory(String id, String type, double latitude, double longtitude, double altitude) {

		this.ID = id;
		this.setLatitude(latitude);
		this.setLongtitude(longtitude);
		this.setAltitude(altitude);
		this.setType(type);

	}

	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongtitude() {
		return longtitude;
	}

	public void setLongtitude(double longtitude) {
		this.longtitude = longtitude;
	}

	public double getAltitude() {
		return altitude;
	}

	public void setAltitude(double altitude2) {
		this.altitude = altitude2;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {

		// TODO: use string builder
		return "\n " + isNew() + getCreatedDateTime() + " Type:ID " + this.getType() + ":" + getID()
				+ " (lat,long,alt):(" + getLatitude() + "," + getLongtitude() + "," + getAltitude() + ") \t "
				+ (getStatistics() != null ? getStatistics() : "  ") + (getSector() != null ? getSector() : " ");
	}

	public TrajectoryStatisticsWrapper getStatistics() {
		return statistics;
	}

	public void setStatistics(TrajectoryStatisticsWrapper statistics) {
		this.statistics = statistics;
	}

	public LocalDateTime getCreatedDateTime() {
		return createdDateTime;
	}

	public void setCreatedDateTime(LocalDateTime createdDateTime) {
		this.createdDateTime = createdDateTime;
	}

	public Sector getSector() {
		return sector;
	}

	public void setSector(Sector sector) {
		this.sector = sector;
	}

	public boolean isNew() {
		return isNew;
	}

	public void setNew(boolean isNew) {
		this.isNew = isNew;
	}

	public long getStreamedTime() {
		return streamedTime;
	}

	public void setStreamedTime(long streamedTime) {
		this.streamedTime = streamedTime;
	}

}
