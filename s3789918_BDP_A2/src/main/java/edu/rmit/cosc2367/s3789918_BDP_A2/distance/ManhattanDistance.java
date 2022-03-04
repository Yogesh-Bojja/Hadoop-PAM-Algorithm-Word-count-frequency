package edu.rmit.cosc2367.s3789918_BDP_A2.distance;

import de.jungblut.math.DoubleVector;

public final class ManhattanDistance implements DistanceMeasurer {

	@Override
	public double measureDistance(double[] set1, double[] set2) {
		double sum = 0;
		int length = set1.length;
		for (int i = 0; i < length; i++) {
			sum += Math.abs(set1[i] - set2[i]);
		}
		return sum;
	}

	@Override
	public double measureDistance(DoubleVector vec1, DoubleVector vec2) {
		return vec1.subtract(vec2).abs().sum();
	}

}