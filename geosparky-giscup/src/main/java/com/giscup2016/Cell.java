package com.giscup2016;

/**
 * Cell
 *
 * @author Ajinkya Patil
 * @version 1.0
 * @since 1.0
 */
public class Cell {
    private int x;
    private int y;
    private int z;
    private int value;

    public Cell() {
    }

    public Cell(int x, int y, int z) {
        this.x = x;
        this.y = y;
        this.z = z;
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

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object o){
    	if(o == this)
    		return true;
    	
    	if(!(o instanceof Cell) )
    		return false;
    	
    	Cell newcell = (Cell)o;
    	
    	return newcell.x == this.x && newcell.y == this.y && newcell.z == this.z;
    }
    
    @Override
    public int hashCode(){
    	String hashStr = this.x + "" + this.y + "" + this.z;
    	return Integer.parseInt(hashStr);
    }
}
