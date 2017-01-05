package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.io.Serializable;

import javax.sound.sampled.Line;

import com.vividsolutions.jts.geom.Polygon;

import de.kdml.bigdatalab.spark_and_flink.common_utils.GeoUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.Utils;

/**
 * The Air sectors
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class Sector implements Serializable {

	private static final long serialVersionUID = 2570359709368571929L;

	private String name;
	private String airBlockName;
	private double lowerFlightLevel;
	private double upperFlightLevel;
	private Polygon polygon;

	public Sector() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAirBlockName() {
		return airBlockName;
	}

	public void setAirBlockName(String airBlockName) {
		this.airBlockName = airBlockName;
	}

	public double getLowerFlightLevel() {
		return lowerFlightLevel;
	}

	public void setLowerFlightLevel(double lowerFlightLevel) {
		this.lowerFlightLevel = lowerFlightLevel;
	}

	public double getUpperFlightLevel() {
		return upperFlightLevel;
	}

	public void setUpperFlightLevel(double upperFlightLevel) {
		this.upperFlightLevel = upperFlightLevel;
	}

	public Polygon getPolygon() {
		return polygon;
	}

	public void setPolygon(Polygon polygon) {
		this.polygon = polygon;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder("");
		str.append("Sector-name:");
		str.append(getName());
		str.append(" block-name:");
		str.append(getAirBlockName());
		str.append(" ");
		str.append(getPolygon());

		return str.toString();
	}

	/**
	 * Parse string line that represents a sector (i.e.
	 * "SECTOR_NAME";"REGION_NAME";"AIRBLOCK_COUNT";"SECTOR_TYPE";"AIRBLOCK_NAME";"LOWER_FL_LEVEL";"UPPER_FL_LEVEL";"BOUNDARY_WKT")
	 * 
	 * @param sectorLine
	 * @return
	 */
	public static Sector parseSectorData(String sectorLine) {

		Sector sector = new Sector();

		String[] attributes = sectorLine.replaceAll("\"", "").split(";");

		sector.setName(attributes[0]);
		sector.setAirBlockName(attributes[4]);
		sector.setLowerFlightLevel(Utils.parseDoubleOrElse(attributes[5], 0));
		sector.setUpperFlightLevel(Utils.parseDoubleOrElse(attributes[6], 0));
		sector.setPolygon(GeoUtils.getPolygon(attributes[7]));
		return sector;
	}
}
