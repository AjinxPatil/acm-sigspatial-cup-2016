package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * GeoHotspotLocator
 *
 * @version 1.0
 * @since 1.0
 */
public class GeoHotspotLocator {
    public static void main(String[] args) {
        // TODO: Remove config
        final SparkConf conf = new SparkConf().setAppName("geosparky-giscup")
                .setMaster("local[1]").set("spark.driver.host", "127.0.0.1");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaPairRDD<Cell, Integer> cellAttrs = sc.textFile("input").filter(line -> isPointValid(line))
                .mapToPair(line -> new Tuple2<>(createCell(line), 1)).reduceByKey((x, y) -> x + y);
        final Double s = calculateSValue(cellAttrs);
        final Broadcast<Double> broadcastS = sc.broadcast(s);
        final JavaPairRDD<Cell, Integer> cellNetAttrValues = calculateCellNetAttrValue(cellAttrs);
        final Integer xBar = calculateXBar(cellAttrs);
        final Broadcast<Integer> broadcastXBar = sc.broadcast(xBar);

        final Broadcast<Integer> broadcastN = sc.broadcast(GeoHotspotConstants.gridCells());
        final JavaPairRDD<Cell, Double> getisOrd = cellNetAttrValues.mapToPair(a -> calculateGetisOrd(a, broadcastS,
                broadcastXBar, broadcastN));
        getisOrd.saveAsTextFile("output");
        sc.close();
    }

    private static List<Tuple2<Cell, Integer>> getCellNeighborAttrValueList(final Cell cell, final Integer attrVal) {
        final int rows = GeoHotspotConstants.gridRows();
        final int columns = GeoHotspotConstants.gridColumns();
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

    private static JavaPairRDD<Cell, Integer> calculateCellNetAttrValue(final JavaPairRDD<Cell, Integer> cellAttrs) {
        final JavaPairRDD<Cell, Integer> neighborAttrValueRdd = cellAttrs.flatMapToPair(a ->
                getCellNeighborAttrValueList(a._1(), a._2()).iterator());
        return neighborAttrValueRdd.reduceByKey((a, b) -> a + b);
    }

    private static Double calculateSValue(final JavaPairRDD<Cell, Integer> cellAttrValues) {
        final JavaRDD<Integer> attrValues = cellAttrValues.map(a -> a._2());
        final Integer netAttr = attrValues.reduce((a, b) -> a + b);
        final Integer gridCellCount = GeoHotspotConstants.gridCells();
        final Integer netAttrSquared = attrValues.reduce((a, b) -> a + b * b);
        return Math.sqrt(netAttrSquared / gridCellCount - Math.pow(netAttr / gridCellCount, 2));
    }

    private static Integer calculateXBar(final JavaPairRDD<Cell, Integer> cellAttrValues) {
        return (cellAttrValues.map(a -> a._2()).reduce((a, b) -> a + b)) / GeoHotspotConstants.gridCells();
    }

    private static Tuple2<Cell, Double> calculateGetisOrd(final Tuple2<Cell, Integer> getisOrdParameters,
                                                          final Broadcast<Double> broadcastS, final
                                                          Broadcast<Integer> broadcastXBar, final Broadcast<Integer>
                                                                  broadcastN) {
        final Integer numberOfNeighbours = getisOrdParameters._1().getNn();
        final Double getisOrd = (getisOrdParameters._2() - (broadcastXBar.value() * numberOfNeighbours)) /
                (broadcastS.value() * Math.sqrt(((broadcastN.value() * numberOfNeighbours) - (numberOfNeighbours *
                        numberOfNeighbours)) /
                        (broadcastN.value() - 1)));
        return new Tuple2<>(getisOrdParameters._1(), getisOrd);
    }

    private static boolean isPointValid(final String line) {
        final String[] fields = line.split(",");
        final double lat = Double.parseDouble(fields[6]);
        final double lon = Double.parseDouble(fields[5]);
        return lat >= GeoHotspotConstants.LATITUDE_MIN && lat <= GeoHotspotConstants.LATITUDE_MAX
                && lon >= GeoHotspotConstants.LONGITUDE_MIN && lon <= GeoHotspotConstants.LONGITUDE_MAX;
    }

    private static Cell createCell(final String line) {
        final String[] fields = line.split(",");
        final int x = (int) Math.floor(100.0 * (Double.parseDouble(fields[6]) - GeoHotspotConstants.LATITUDE_MIN));
        final int y = (int) Math.floor(100.0 * Math.abs(Double.parseDouble(fields[5]) - GeoHotspotConstants
                .LONGITUDE_MIN));
        final int z = Integer.parseInt(fields[1].split(" ")[0].split("-")[2]);
        return new Cell(x, y, z);
    }

}
