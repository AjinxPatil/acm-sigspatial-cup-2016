package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class GeoHotspotLocator {
    public static void main(String[] args) {
        // TODO: Remove config
        final SparkConf conf = new SparkConf().setAppName("geosparky-giscup")
                .setMaster("local[1]").set("spark.driver.host", "127.0.0.1");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaPairRDD<Cell, Integer> cellAttrs = sc.textFile("input").filter(line -> isValid(line))
                .mapToPair(line -> new Tuple2<>(createCell(line), 1)).reduceByKey((x, y) -> x + y);
        final Double s = calculateSValue(cellAttrs);
        final JavaPairRDD<Cell, Integer> cellNetAttrValues = calculateCellNetAttrValue(cellAttrs);
        final JavaPairRDD<Cell, Integer> cellNeighborCount = calculateCellNeighborCount(cellAttrs);

        cellAttrs.saveAsTextFile("output");
        sc.close();
    }

    private static List<Tuple2<Cell, Integer>> getCellNeighborAttrValueList(final Cell cell, final Integer attrVal) {
        final int rows = GeoHotspotConstants.countGridRows();
        final int columns = GeoHotspotConstants.countGridColumns();
        final List<Tuple2<Cell, Integer>> neighborAttrValueList = new ArrayList<>();
        final int x = cell.getX();
        final int y = cell.getY();
        final int z = cell.getZ();
        for (int i = x - 1; i <= x + 1; i++) {
            for (int j = y - 1; j <= y + 1; j++) {
                for (int k = z - 1; k <= z + 1; k++) {
                    if (i >= 0 && i <= rows && j >= 0 && j <= columns && k >= 1 && k <= GeoHotspotConstants.DAYS_MAX) {
                        neighborAttrValueList.add(new Tuple2<>(new Cell(i, j, k), attrVal));
                    }
                }
            }
        }
        return neighborAttrValueList;
    }

    private static Integer getCellNeighborCount(final Cell cell) {
        final int rows = GeoHotspotConstants.countGridRows();
        final int columns = GeoHotspotConstants.countGridColumns();
        final List<Tuple2<Cell, Integer>> neighborAttrValueList = new ArrayList<>();
        final int x = cell.getX();
        final int y = cell.getY();
        final int z = cell.getZ();
        int count = 0;
        for (int i = x - 1; i <= x + 1; i++) {
            for (int j = y - 1; j <= y + 1; j++) {
                for (int k = z - 1; k <= z + 1; k++) {
                    if (i >= 0 && i <= rows && j >= 0 && j <= columns && k >= 1 && k <= GeoHotspotConstants.DAYS_MAX) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private static JavaPairRDD<Cell, Integer> calculateCellNetAttrValue(final JavaPairRDD<Cell, Integer> cellAttrs) {
        final JavaPairRDD<Cell, Integer> neighborAttrValueRdd = cellAttrs.flatMapToPair(a ->
                getCellNeighborAttrValueList(a._1(), a._2()).iterator());
        return neighborAttrValueRdd.reduceByKey((a, b) -> a + b);
    }

    private static JavaPairRDD<Cell, Integer> calculateCellNeighborCount(final JavaPairRDD<Cell, Integer> cellAttrs) {
        return cellAttrs.mapToPair(a -> new Tuple2<>(a._1(), getCellNeighborCount(a._1())));
    }


    private static Double calculateSValue(JavaPairRDD<Cell, Integer> cellAttrValues) {
        final JavaRDD<Integer> attrValues = cellAttrValues.map(a -> a._2());
        final Integer netAttr = attrValues.reduce((a, b) -> a + b);
        final Integer gridCellCount = GeoHotspotConstants.countGridCells();
        final Integer netAttrSquared = attrValues.reduce((a, b) -> a + b * b);
        return Math.sqrt(netAttrSquared / gridCellCount - Math.pow(netAttr / gridCellCount, 2));
    }

    public static boolean isValid(String line) {
        String[] input = line.split(",");
        double latitude = Double.parseDouble(input[6]);
        double longitude = Double.parseDouble(input[5]);
        return (latitude >= GeoHotspotConstants.MIN_LATITUDE && latitude <= GeoHotspotConstants.MAX_LATITUDE
                && longitude >= GeoHotspotConstants.MIN_LONGITUDE && longitude <= GeoHotspotConstants.MAX_LONGITUDE);
    }

    public static Cell createCell(String line) {
        String[] input = line.split(",");
        double lat = Double.parseDouble(input[6]);
        int latitude = (int) Math.floor(100.0 * (lat - GeoHotspotConstants.MIN_LATITUDE));
        double lon = Double.parseDouble(input[5]);
        int longitude = (int) Math.floor(100.0 * Math.abs(lon - GeoHotspotConstants.MIN_LONGITUDE));
        int day = Integer.parseInt(input[1].split(" ")[0].split("-")[2]);
        return new Cell(latitude, longitude, day);
    }

}
