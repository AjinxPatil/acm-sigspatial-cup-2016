package com.giscup2016;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * GeoHotspotLocator
 *
 * @version 1.0
 * @since 1.0
 */
public class GeoHotspotLocator {
	
	static class GetisOrdComparator implements Serializable, Comparator<Tuple2<Cell, Double>> {

		@Override
		public int compare(Tuple2<Cell, Double> o1, Tuple2<Cell, Double> o2) {
			return o1._2.compareTo(o2._2);
		}
		
	}
	
    public static void main(String[] args) {
        // TODO: Remove config
        final SparkConf conf = new SparkConf().setAppName("geosparky-giscup");
                //.setMaster("local[1]").set("spark.driver.host", "127.0.0.1");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaPairRDD<Cell, Long> cellAttrs = sc.textFile(args[0]).filter(line -> isPointValid(line))
                .mapToPair(line -> new Tuple2<>(createCell(line), 1L)).reduceByKey((x, y) -> x + y);
        final Double s = calculateSValue(cellAttrs);
        final Broadcast<Double> broadcastS = sc.broadcast(s);
        final JavaPairRDD<Cell, Long> cellNetAttrValues = calculateCellNetAttrValue(cellAttrs);
        final Double xBar = calculateXBar(cellAttrs);
        final Broadcast<Double> broadcastXBar = sc.broadcast(xBar);
        final Broadcast<Integer> broadcastN = sc.broadcast(GeoHotspotConstants.gridCells());
        final JavaPairRDD<Cell, Double> getisOrd = cellNetAttrValues.mapToPair(a -> calculateGetisOrd(a, broadcastS,
                broadcastXBar, broadcastN));
        List<Tuple2<Cell, Double>> getisOrdTopFifty = getisOrd.top(50, new GetisOrdComparator());
        final JavaRDD<Tuple2<Cell, Double>> getisOrd50 = sc.parallelize(getisOrdTopFifty);
        getisOrd50.saveAsTextFile(args[1]);
        sc.close();
    }

    private static List<Tuple2<Cell, Long>> getCellNeighborAttrValueList(final Cell cell, final Long attrVal) {
        final int rows = GeoHotspotConstants.gridRows();
        final int columns = GeoHotspotConstants.gridColumns();
        final List<Tuple2<Cell, Long>> neighborAttrValueList = new ArrayList<>();
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

    private static JavaPairRDD<Cell, Long> calculateCellNetAttrValue(final JavaPairRDD<Cell, Long> cellAttrs) {
        final JavaPairRDD<Cell, Long> neighborAttrValueRdd = cellAttrs.flatMapToPair(a ->
                getCellNeighborAttrValueList(a._1(), a._2()).iterator());
        return neighborAttrValueRdd.reduceByKey((a, b) -> a + b);
    }

    private static Double calculateSValue(final JavaPairRDD<Cell, Long> cellAttrValues) {
        final JavaRDD<Long> attrValues = cellAttrValues.map(a -> a._2());
        final Long netAttr = attrValues.reduce((a, b) -> a + b);
        final Integer gridCellCount = GeoHotspotConstants.gridCells();
        final Long netAttrSquared = attrValues.map(a -> a * a).reduce((a, b) -> a + b);
        return Math.sqrt(netAttrSquared / (1.0 * gridCellCount) - Math.pow(netAttr / (1.0 * gridCellCount), 2));
    }

    private static Double calculateXBar(final JavaPairRDD<Cell, Long> cellAttrValues) {
        return (cellAttrValues.map(a -> a._2()).reduce((a, b) -> a + b)) / (1.0 * GeoHotspotConstants.gridCells());
    }

    private static Tuple2<Cell, Double> calculateGetisOrd(final Tuple2<Cell, Long> getisOrdParameters,
                                                          final Broadcast<Double> broadcastS, final
                                                          Broadcast<Double> broadcastXBar, final Broadcast<Integer>
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
