package com.giscup2016;

/**
 * Constants class for GeoHotspotLocator
 *
 * @version 1.0
 * @since 1.0
 */
public class GeoHotspotConstants {
    public static final double LONGITUDE_MIN = -74.25;
    public static final double LONGITUDE_MAX = -73.7;
    public static final double LATITUDE_MIN = 40.5;
    public static final double LATITUDE_MAX = 40.9;
    public static final int DAYS_MAX = 31;
    public static final double CELL_X = 0.01;
    public static final double CELL_Y = 0.01;
    public static final double CELL_Z = 1;

    public static int gridRows() {
        return (int) ((LONGITUDE_MAX - LONGITUDE_MIN) / CELL_X);
    }

    public static int gridColumns() {
        return (int) ((LATITUDE_MAX - LATITUDE_MIN) / CELL_Y);
    }

    public static int gridCells() {
        final int rows = (int) ((LONGITUDE_MAX - LONGITUDE_MIN) / CELL_X);
        final int columns = (int) ((LATITUDE_MAX - LATITUDE_MIN) / CELL_Y);
        return rows * columns * DAYS_MAX;
    }
}