package edu.rmit.cosc2367.s3789918_BDP_A2.distance;

import de.jungblut.math.DoubleVector;

public interface DistanceMeasurer {

	public double measureDistance(double[] set1, double[] set2);

	public double measureDistance(DoubleVector vec1, DoubleVector vec2);

}