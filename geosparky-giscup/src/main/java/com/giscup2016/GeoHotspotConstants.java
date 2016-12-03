package com.giscup2016;

/**
 * Constants class for GeoHotspotLocator
 *
 * @version 1.0
 * @since 1.0
 */
public class GeoHotspotConstants {
    public static final int LONGITUDE_MIN = -7425;
    public static final int LONGITUDE_MAX = -7370;
    public static final int LATITUDE_MIN = 4050;
    public static final int LATITUDE_MAX = 4090;
    public static final int DAYS_MAX = 31;

    public static int gridRows() {
        return LONGITUDE_MAX - LONGITUDE_MIN;
    }

    public static int gridColumns() {
        return LATITUDE_MAX - LATITUDE_MIN;
    }

    public static int gridCells() {
        return gridRows() * gridColumns() * DAYS_MAX;
    }
}