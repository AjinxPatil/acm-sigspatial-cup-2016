package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class GeoHotspotLocator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GeoHotspotLocator")
				.setMaster("local[1]").set("spark.driver.host","127.0.0.1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Cell, Integer> trips = sc.textFile("yellow_tripdata_2015-01_slice.csv").filter(line -> isValid(line))
				.mapToPair(line -> new Tuple2<Cell, Integer>(createCell(line),1)).reduceByKey((x, y) -> x + y );
		trips.saveAsTextFile("output");
		sc.close();
	}
	
	public static boolean isValid(String line) {
		String[] input = line.split(",");
		double latitude = Double.parseDouble(input[6]);
		double longitude = Double.parseDouble(input[5]);
		return (latitude >= GeoHostspotConstants.MIN_LATITUDE && latitude <= GeoHostspotConstants.MAX_LATITUDE
				&& longitude >= GeoHostspotConstants.MIN_LONGITUDE && longitude <= GeoHostspotConstants.MAX_LONGITUDE);
	}
	
	public static Cell createCell(String line) {
		String[] input = line.split(",");
		double lat = Double.parseDouble(input[6]);
		int latitude = (int)Math.floor(100.0 * (lat - GeoHostspotConstants.MIN_LATITUDE));
		double lon = Double.parseDouble(input[5]);
		int longitude = (int) Math.floor(100.0 * Math.abs(lon - GeoHostspotConstants.MIN_LONGITUDE));
		int day = Integer.parseInt(input[1].split(" ")[0].split("-")[2]);
		return new Cell(latitude,longitude,day);
	}
	
}
