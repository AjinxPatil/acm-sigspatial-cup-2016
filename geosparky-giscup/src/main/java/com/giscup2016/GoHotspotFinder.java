package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple4;

/**
 * GoHotspotFinder
 *
 * @author Ajinkya Patil
 * @version 1.0
 * @since 1.0
 */
public class GoHotspotFinder {
    private static boolean isPointValid(final String line, final Tuple4<Double, Double, Double, Double> envelope) {
        final String[] columns = line.split(",");
        final Double longi = Double.valueOf(columns[5]);
        final Double lat = Double.valueOf(columns[6]);
        if (longi < envelope._1() || longi > envelope._2()) {
            return false;
        }
        if (lat < envelope._3() || lat > envelope._4()) {
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
        final int z = Integer.valueOf(day);
        return new Cell(x, y, z);
    }

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("geospark-giscup");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        Tuple4<Double, Double, Double, Double> envelope = new Tuple4<>(GoHostspotConstants.MIN_LONGI,
                GoHostspotConstants.MAX_LONGI, GoHostspotConstants.MIN_LAT, GoHostspotConstants.MAX_LAT);

        final JavaRDD<Cell> cabdata = sc.textFile("input").filter(line -> isPointValid(line, envelope)).map(line ->
                constructCell(line));
    }
}
