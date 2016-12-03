package com.giscup2016;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * GeoHotspotLocator
 *
 * @version 1.0
 * @since 1.0
 */
public class GeoHotspotLocator implements Serializable {

    private static class GetisOrdComparator implements Serializable, Comparator<Tuple2<Cell, Double>> {
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

        final JavaPairRDD<Cell, Long> cellAttrs = sc.textFile(args[0]).mapToPair(line -> new Tuple2<>(createCell
                (line), 1L)).filter(tuple -> isCellValid(tuple._1)).reduceByKey((x, y) -> x + y);

        final double xBar = calculateXBar(cellAttrs);
        final double s = calculateS(cellAttrs, xBar);

        final JavaPairRDD<Cell, Long> cellNetAttrValues = calculateCellNetAttrValue(cellAttrs);

        final JavaPairRDD<Cell, Double> getisOrd = cellNetAttrValues.mapToPair(a -> calculateGetisOrd(a, s, xBar));
        final List<Tuple2<Cell, Double>> getisOrdTopFifty = getisOrd.top(50, new GetisOrdComparator());
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

    private static Double calculateXBar(final JavaPairRDD<Cell, Long> cellAttrValues) {
        return (cellAttrValues.map(a -> a._2).reduce((a, b) -> a + b)) / ((double) GeoHotspotConstants.gridCells());
    }

    private static Double calculateS(final JavaPairRDD<Cell, Long> cellAttrValues, final double xBar) {
        final JavaRDD<Long> attrValues = cellAttrValues.map(a -> a._2());
        final long netAttrSquared = attrValues.map(a -> a * a).reduce((a, b) -> a + b);
        return Math.sqrt(netAttrSquared / ((double) GeoHotspotConstants.gridCells()) - Math.pow(xBar, 2));
    }

    private static Tuple2<Cell, Double> calculateGetisOrd(final Tuple2<Cell, Long> cellNetAttrValues, final double
            s, final double xBar) {
        final int numberOfNeighbours = cellNetAttrValues._1().getNn();
        final int N = GeoHotspotConstants.gridCells();
        final Double getisOrd = (cellNetAttrValues._2() - xBar * numberOfNeighbours) /
                (s * Math.sqrt((N * numberOfNeighbours - numberOfNeighbours * numberOfNeighbours) / (N - 1)));
        return new Tuple2<>(cellNetAttrValues._1(), getisOrd);
    }

    private static boolean isCellValid(final Cell cell) {
        return cell.getX() >= 0 && cell.getX() <= GeoHotspotConstants.gridColumns() &&
                cell.getY() >= 0 && cell.getY() <= GeoHotspotConstants.gridRows();
    }

    private static Cell createCell(final String line) {
        final String[] fields = line.split(",");
        final int lat = (int) (100.0 * Double.parseDouble(fields[6]));
        final int lon = (int) (100.0 * Double.parseDouble(fields[5]));
        final int x = lat - GeoHotspotConstants.LATITUDE_MIN;
        final int y = -1 * (lon - GeoHotspotConstants.LONGITUDE_MIN);
        final int z = Integer.parseInt(fields[1].split(" ")[0].split("-")[2]);
        return new Cell(x, y, z);
    }

}
