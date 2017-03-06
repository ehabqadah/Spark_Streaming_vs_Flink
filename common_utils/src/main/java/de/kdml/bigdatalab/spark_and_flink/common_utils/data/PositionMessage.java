package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Position of Aircraft follows the Automatic Dependent Surveillance–Broadcast
 * 
 * (ADS-B) messages schema, with additional fields.
 * 
 * @author Ehab Qadah
 * 
 *         Feb 22, 2017
 */

public class PositionMessage implements Serializable {

	private static final long serialVersionUID = 1013765199466780042L;
	private String ID;
	private double latitude;
	private double longitude;
	private double altitude;
	private String type;
	private TrajectoryStatisticsWrapper statistics;
	private LocalDateTime createdDateTime;
	private Sector sector;
	private Sector prevSector;
	private Boolean isNew = null;
	private long streamedTime, finishProcessingTime;
	private Double ditance = null;
	private Double speed = null;
	private Double acceleration;

	public PositionMessage() {
	}

	public PositionMessage(String id, String type, double latitude, double longtitude, double altitude) {

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
		return "|" + isNew() + getCreatedDateTime() + ":(" + getLatitude() + "," + getLongitude() + "," + getAltitude()
				+ ") |" + " speed=" + getSpeed() + " Distance=" + getDistance() + "|"
				+ (getStatistics() != null ? getStatistics() : "  ") + (getSector() != null ? getSector() : " ") + "\n";
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

	public Double getDistance() {
		return ditance;
	}

	public void setDistance(Double ditance) {
		this.ditance = ditance;
	}

	public Double getSpeed() {
		return speed;
	}

	public void setSpeed(Double speed) {
		this.speed = speed;
	}

	public Double getAcceleration() {
		return acceleration;
	}

	public void setAcceleration(Double acceleration) {
		this.acceleration = acceleration;
	}

	public Sector getPrevSector() {
		return prevSector;
	}

	public void setPrevSector(Sector prevSector) {
		this.prevSector = prevSector;
	}

	public long getFinishProcessingTime() {
		return finishProcessingTime;
	}

	public void setFinishProcessingTime(long finishProcessingTime) {
		this.finishProcessingTime = finishProcessingTime;
	}

}
