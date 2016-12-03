
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
        //final JavaPairRDD<Cell, Long> cellAttrs = sc.textFile("yellow_tripdata_2015-01.csv", 200)
        final JavaPairRDD<Cell, Long> cellAttrs = sc.textFile(args[0], 200)
                .mapToPair(line -> new Tuple2<>(createCell(line), 1L))
                .filter(tuple -> isPointValid(tuple._1()))
                .reduceByKey((x, y) -> x + y)
                .cache();
        // Broadcast N
        final Broadcast<Integer> broadcastN = sc.broadcast(68200);
        
        // Calculate XBar and broadcast
        final Double xBar = calculateXBar(cellAttrs, broadcastN);
        final Broadcast<Double> broadcastXBar = sc.broadcast(xBar);
        
        // Calculate S and broadcast
        final Double s = calculateSValue(cellAttrs, broadcastXBar, broadcastN);
        final Broadcast<Double> broadcastS = sc.broadcast(s);
        final JavaPairRDD<Cell, Long> cellNetAttrValues = calculateCellNetAttrValue(cellAttrs);
        
        final JavaPairRDD<Cell, Double> getisOrd = cellNetAttrValues.mapToPair(a -> calculateGetisOrd(a, broadcastS,
                broadcastXBar, broadcastN));
        List<Tuple2<Cell, Double>> getisOrdTopFifty = getisOrd.top(50, new GetisOrdComparator());
        final JavaRDD<Tuple2<Cell, Double>> getisOrd50 = sc.parallelize(getisOrdTopFifty);
        //getisOrd50.saveAsTextFile("output");
        getisOrd50.saveAsTextFile(args[1]);
        sc.close();
        System.out.println(GeoHotspotConstants.gridColumns()+" "+GeoHotspotConstants.gridRows()+" "+GeoHotspotConstants.gridCells());
        System.out.println(GeoHotspotConstants.DAYS_MAX);
    }

    private static List<Tuple2<Cell, Long>> getCellNeighborAttrValueList(final Cell cell, final Long attrVal) {
        final int rows = 54;
        final int columns = 39;
        final List<Tuple2<Cell, Long>> neighborAttrValueList = new ArrayList<>();
        final int x = cell.getX();
        final int y = cell.getY();
        final int z = cell.getZ();
        for (int i = x - 1; i <= x + 1; i++) {
            for (int j = y - 1; j <= y + 1; j++) {
                for (int k = z - 1; k <= z + 1; k++) {
                    if (i >= 0 && i <= rows && j >= 0 && j <= columns && k >= 1 && k <= 31) {
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

    private static Double calculateSValue(final JavaPairRDD<Cell, Long> cellAttrValues, Broadcast<Double> broadcastXBar, Broadcast<Integer> broadcastN) {
        final JavaRDD<Long> attrValues = cellAttrValues.map(a -> a._2());
        final Long netAttrSquared = attrValues.map(a -> a * a).reduce((a, b) -> a + b);
        return Math.sqrt(netAttrSquared / (1.0 * broadcastN.getValue()) - broadcastXBar.getValue());
    }

    private static Double calculateXBar(final JavaPairRDD<Cell, Long> cellAttrValues, Broadcast<Integer> broadcastN) {
        return (cellAttrValues.map(a -> a._2()).reduce((a, b) -> a + b)) / (1.0 * broadcastN.getValue());
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

    private static boolean isPointValid(final Cell cell) {
        return cell.getX() >= 0 && cell.getX() <= 39
                && cell.getY() >= 0 && cell.getY() <= 54;
    }

    private static Cell createCell(final String line) {
        final String[] fields = line.split(",");
        final int x = (int) Math.floor(100.0 * (Double.parseDouble(fields[6]) - GeoHotspotConstants.LATITUDE_MIN));
        final int y = (int) (GeoHotspotConstants.LONGITUDE_MIN * -100.0) + (int) (100.0 * Double.parseDouble(fields[5]));
        final int z = Integer.parseInt(fields[1].split(" ")[0].split("-")[2]);
        return new Cell(x, y, z);
    }

}
