package com.giscup2016;

import java.io.Serializable;

/**
 * Cell
 *
 * @version 1.0
 * @since 1.0
 */
public class Cell implements Serializable {
    private int x;
    private int y;
    private int z;
    private int nn;

    public Cell() {
    }

    public Cell(int x, int y, int z) {
        this.x = x;
        this.y = y;
        this.z = z;
        setNn();
    }

    public int getX() {

        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getZ() {
        return z;
    }

    public void setZ(int z) {
        this.z = z;
    }

    private void setNn() {
        final int rows = GeoHotspotConstants.gridRows();
        final int columns = GeoHotspotConstants.gridColumns();
        final int x = this.getX();
        final int y = this.getY();
        final int z = this.getZ();
        for (int i = x - 1; i <= x + 1; i++) {
            for (int j = y - 1; j <= y + 1; j++) {
                for (int k = z - 1; k <= z + 1; k++) {
                    if (i >= 0 && i <= rows && j >= 0 && j <= columns && k >= 1 && k <= GeoHotspotConstants.DAYS_MAX) {
                        this.nn++;
                    }
                }
            }
        }
    }

    public int getNn() {
        return nn;
    }

    @Override
    public String toString() {
        return "(" + -(7425-y) + ", " + (x+4050) + ", " + z + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof Cell))
            return false;

        Cell newcell = (Cell) o;

        return newcell.x == this.x && newcell.y == this.y && newcell.z == this.z;
    }

    @Override
    public int hashCode() {
        String hashStr = this.x + "" + this.y + "" + this.z;
        return Integer.parseInt(hashStr);
    }
}
