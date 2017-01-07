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

	/**
	 * Calculate distance between two points in latitude and longitude taking
	 * into account height difference. If you are not interested in height
	 * difference pass 0.0. Uses Haversine method as its base.
	 * 
	 * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
	 * el2 End altitude in meters
	 * 
	 * @returns Distance in Meters
	 * 
	 *          {@link http://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude-what-am-i-doi}
	 * 
	 **/
	public static double distance(double lat1, double lat2, double lon1, double lon2, double altit1, double altit2) {

		final int R = 6371; // Radius of the earth

		Double latDistance = Math.toRadians(lat2 - lat1);
		Double lonDistance = Math.toRadians(lon2 - lon1);
		Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = R * c * 1000; // convert to meters

		double height = altit1 - altit2;

		distance = Math.pow(distance, 2) + Math.pow(height, 2);

		return Math.sqrt(distance);
	}

	/**
	 * 
	 * @param lat1
	 * @param lon1
	 * @param lat2
	 * @param long2
	 * @return
	 */
	public static double greatCircleDistance(double lat1, double lon1, double lat2, double long2) {
		final double la1 = Math.toRadians(lat1);
		final double lo1 = Math.toRadians(lon1);
		final double la2 = Math.toRadians(lat2);
		final double lo2 = Math.toRadians(long2);

		return 6372.795 * 2 * Math.asin(Math.sqrt(Math.pow(Math.sin((la2 - la1) / 2), 2)
				+ Math.cos(la1) * Math.cos(la2) * Math.pow(Math.sin((lo2 - lo1) / 2), 2)));
	}

}
