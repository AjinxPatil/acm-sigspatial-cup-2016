package com.giscup2016;

/**
 * Constants class for GoHotspotFinder
 *
 * @author Ajinkya Patil
 * @version 1.0
 * @since 1.0
 */
public class GeoHotspotConstants {
    public static final double MIN_LONGITUDE = -74.25;
    public static final double MAX_LONGITUDE = -73.7;
    public static final double MIN_LATITUDE = 40.5;
    public static final double MAX_LATITUDE = 40.9;
    public static final int DAYS_MAX = 31;
    public static final double CELL_X = 0.01;
    public static final double CELL_Y = 0.01;
    public static final double CELL_Z = 1;

    public static int countGridRows() {
        return (int) ((MAX_LONGITUDE - MIN_LONGITUDE) / CELL_X);
    }

    public static int countGridColumns() {
        return (int) ((MAX_LATITUDE - MIN_LATITUDE) / CELL_Y);
    }

    public static int countGridCells() {
        final int rows = (int) ((MAX_LONGITUDE - MIN_LONGITUDE) / CELL_X);
        final int columns = (int) ((MAX_LATITUDE - MIN_LATITUDE) / CELL_Y);
        return rows * columns * DAYS_MAX;
    }
}