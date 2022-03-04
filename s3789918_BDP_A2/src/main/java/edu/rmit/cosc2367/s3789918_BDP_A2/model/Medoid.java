package edu.rmit.cosc2367.s3789918_BDP_A2.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import de.jungblut.math.DoubleVector;

public class Medoid implements WritableComparable<Medoid> {

	private DoubleVector center;
	private int kTimesIncremented = 1;
	private int clusterIndex;

	// Constructors
	public Medoid() {
		super();
	}

	public Medoid(DoubleVector center) {
		super();
		this.center = center.deepCopy();
	}

	public Medoid(Medoid center) {
		super();
		this.center = center.center.deepCopy();
		this.kTimesIncremented = center.kTimesIncremented;
	}

	public Medoid(DataPoint center) {
		super();
		this.center = center.getVector().deepCopy();
	}

	// plus
	public final void plus(DataPoint c) {
		plus(c.getVector());
	}

	public final void plus(DoubleVector c) {
		center = center.add(c);
		kTimesIncremented++;
	}

	public final void plus(Medoid c) {
		kTimesIncremented += c.kTimesIncremented;
		center = center.add(c.getCenterVector());
	}

	// divide
	public final void divideByK() {
		center = center.divide(kTimesIncremented);
	}

	// update
	public final boolean update(Medoid c) {
		return calculateError(c.getCenterVector()) > 0;
	}

	public final double calculateError(DoubleVector v) {
		return Math.sqrt(center.subtract(v).abs().sum());
	}

	// write method
	@Override
	public final void write(DataOutput out) throws IOException {
		DataPoint.writeVector(center, out);
		out.writeInt(kTimesIncremented);
		out.writeInt(clusterIndex);
	}

	// readfields method
	@Override
	public final void readFields(DataInput in) throws IOException {
		this.center = DataPoint.readVector(in);
		kTimesIncremented = in.readInt();
		clusterIndex = in.readInt();
	}

	// compare method
	@Override
	public final int compareTo(Medoid o) {
		return Integer.compare(clusterIndex, o.clusterIndex);
	}

	/**
	 * @return the center
	 */
	public final DoubleVector getCenterVector() {
		return center;
	}

	/**
	 * @return the index of the cluster in a datastructure.
	 */
	public int getClusterIndex() {
		return clusterIndex;
	}

	public void setClusterIndex(int clusterIndex) {
		this.clusterIndex = clusterIndex;
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((center == null) ? 0 : center.hashCode());
		return result;
	}

	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Medoid other = (Medoid) obj;
		if (center == null) {
			if (other.center != null)
				return false;
		} else if (!center.equals(other.center))
			return false;
		return true;
	}

	@Override
	public final String toString() {
		return "Medoid [center=" + center + "]";
	}

}

