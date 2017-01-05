package de.kdml.bigdatalab.spark_and_flink.common_utils;

import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class GeoUtils {

	public static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);

	/**
	 * Get Polygon Well-Known Text format
	 * 
	 * @param polygonWKT
	 * @return
	 */
	public static Polygon getPolygon(String polygonWKT) {

		Polygon polygon = null;

		WKTReader reader = new WKTReader(geometryFactory);
		try {
			polygon = (Polygon) reader.read(polygonWKT);
		} catch (ParseException e) {
			System.out.println("unable to parse polygon" + e.getMessage());
			return null;
		}

		return polygon;

	}

	/**
	 * Check if the point in the polygon
	 * 
	 * @param polygon
	 * @param longtitude
	 * @param latitude
	 * @return
	 */
	public static boolean isPointInPolygon(Polygon polygon, double longtitude, double latitude) {

		Coordinate coord = new Coordinate(longtitude, latitude);
		Point point = geometryFactory.createPoint(coord);

		return polygon.intersection(point).equals(point);
	}
}
