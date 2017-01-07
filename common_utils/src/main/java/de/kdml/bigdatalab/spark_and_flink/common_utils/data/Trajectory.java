package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Trajectory implements Serializable {

	private static final long serialVersionUID = 1013765199466780042L;
	private String ID;
	private double latitude;
	private double longitude;
	private double altitude;
	private String type;
	private TrajectoryStatisticsWrapper statistics;
	private LocalDateTime createdDateTime;
	private Sector sector;
	private Boolean isNew=null;
	private long streamedTime;

	public Trajectory() {
	}

	public Trajectory(String id, String type, double latitude, double longtitude, double altitude) {

		this.ID = id;
		this.setLatitude(latitude);
		this.setLongitude(longtitude);
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

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longtitude) {
		this.longitude = longtitude;
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
				+ " (lat,long,alt):(" + getLatitude() + "," + getLongitude() + "," + getAltitude() + ") \t "
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

	public Boolean isNew() {
		return isNew;
	}

	public void setNew(Boolean isNew) {
		this.isNew = isNew;
	}

	public long getStreamedTime() {
		return streamedTime;
	}

	public void setStreamedTime(long streamedTime) {
		this.streamedTime = streamedTime;
	}

}
