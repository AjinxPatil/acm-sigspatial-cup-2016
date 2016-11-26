package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * GoHotspotFinder
 *
 * @author Ajinkya Patil
 * @version 1.0
 * @since 1.0
 */
public class GoHotspotFinder {
    private static boolean isPointValid(final String line) {
        final String[] columns = line.split(",");
        final Double longi = Double.valueOf(columns[5]);
        final Double lat = Double.valueOf(columns[6]);
        if (longi < GoHostspotConstants.MIN_LONGI || longi > GoHostspotConstants.MAX_LONGI) {
            return false;
        }
        if (lat < GoHostspotConstants.MIN_LAT || lat > GoHostspotConstants.MAX_LAT) {
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

        final int x = (int) ((GoHostspotConstants.MAX_LONGI - longi) / GoHostspotConstants.CELL_X);
        final int y = (int) ((GoHostspotConstants.MAX_LAT - lat) / GoHostspotConstants.CELL_Y);
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
