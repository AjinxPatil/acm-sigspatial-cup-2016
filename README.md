# ACM Sigspatial Cup 2016
#### Distributed computation on Geo-spatial data
-------

This repository contains an implementation for the problem statement of [ACM Signspatial 2016](http://sigspatial2016.sigspatial.org/giscup2016/problem) to calculate the top 50 hotspots for New York city cabs with some constraints.
- The location considered for this problem was the pickup location
- The cell attribute value was the number of journeys instead of the number of passengers
- The step size was of 1 day
- The input data was restricted to January 2015
- Neighbor weight was equal and fixed to 1

The metric for calculation of the hotspots was the Getis-Ord score and the implementation was done on Apache Spark.

## Algorithm

The algorithm we developed allowed complete parallelization of the computation of the Getis Ord score using 2 Mapreduce phases:

1. Mapreduce Phase 1: Creation of cells
  1. Map step: Parse each row from the input file and output a cell object having ID as the X (Latitude), Y (Longitude) and Z (Day) coordinates of the journey
  2. Filter those cells which were outside a fixed geo-envelope
  3. Reduce step: Add up all the journeys in a cell to calculate the attribute value for the cell
2. Intermediate Calculations
  1. Calculate the mean and standard deviation from the cell information
  2. Calculate the total number of cells in the cube
3. Mapreduce Phase 2: Calculating the Getis-Ord score
  1. The key step in the calculation of the Getis-Ord score was finding the neighboring cells and obtaining their attribute value.
  2. The magic sauce to getting the neighbor attribute value was as follows:
    1. Map step: For each cell, calculate the IDs of its neighbors and send out its attribute value to all of its neighbors
    2. Reduce step: For each cell during the reduce step, all of its neighbor values would have sent their values to this cell making all required data for calculating the Getis-Ord score available. The computation of the score was done locally for each cell
4. Retrieving top 50 hotspot cells
  1. The top 50 hotspots were retrieved using the top function of Apache Spark since this was an efficient way to lookup and retrieve top-k values from an RDD

