package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *  ******** Deprecated ********
 * GoHotspotFinder
 * @author Ajinkya Patil
 * @version 1.0
 * @since 1.0
 */
public class GoHotspotFinder {
    private static boolean isPointValid(final String line) {
        final String[] columns = line.split(",");
        final Double longi = Double.valueOf(columns[5]);
        final Double lat = Double.valueOf(columns[6]);
        if (longi < GeoHostspotConstants.MIN_LONGITUDE || longi > GeoHostspotConstants.MIN_LONGITUDE) {
            return false;
        }
        if (lat < GeoHostspotConstants.MIN_LATITUDE || lat > GeoHostspotConstants.MIN_LATITUDE) {
            return false;
        }
        return true;
    }

    private static Cell constructCell(String line) {
        final String[] columns = line.split(",");
        final double longi = Double.parseDouble(columns[5]);
        final double lat = Double.parseDouble(columns[6]);
        // Hardcoding time unit as day
        final String day = columns[1].split(" ")[0].split("-")[2];
        final int x = (int) ((GeoHostspotConstants.MAX_LONGITUDE - longi) / GeoHostspotConstants.CELL_X);
        final int y = (int) ((GeoHostspotConstants.MAX_LATITUDE - lat) / GeoHostspotConstants.CELL_Y);
        final int z = Integer.parseInt(day);
        return new Cell(x, y, z);
    }

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("geospark-giscup");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<Cell> cabdata = sc.textFile("input").filter(line -> isPointValid(line)).map(line ->
                constructCell(line));
    }
}
