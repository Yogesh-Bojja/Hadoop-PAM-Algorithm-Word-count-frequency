package edu.rmit.cosc2367.s3789918_BDP_A2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


import de.jungblut.math.DoubleVector;
import edu.rmit.cosc2367.s3789918_BDP_A2.distance.DistanceMeasurer;
import edu.rmit.cosc2367.s3789918_BDP_A2.distance.EuclidianDistance;
import edu.rmit.cosc2367.s3789918_BDP_A2.model.DataPoint;
import edu.rmit.cosc2367.s3789918_BDP_A2.model.Medoid;

// Reducer Function
public class Yogesh_Task3_Reducer extends Reducer<Medoid, DataPoint, Medoid, DataPoint> {
	public static enum Counter {
		CONVERGED
	}
	
	private final List<Medoid> centers = new ArrayList<>();
	private int iteration = 0;
	private DistanceMeasurer distanceMeasurer;
	private static final Log LOG = LogFactory.getLog(Yogesh_Task3_Reducer.class);
	
	@Override
	protected void reduce(Medoid medoid, Iterable<DataPoint> dataPoints, Context context) throws IOException,
			InterruptedException {

		distanceMeasurer = new EuclidianDistance();
		double oldMedoid_DP_cost = 0;
		List<DataPoint> vectorList = new ArrayList<>();
		
		for (DataPoint value : dataPoints) {
			vectorList.add(new DataPoint(value));
			double c = distanceMeasurer.measureDistance(medoid.getCenterVector(), value.getVector());
			oldMedoid_DP_cost = oldMedoid_DP_cost + c;
		}
		
		oldMedoid_DP_cost = oldMedoid_DP_cost/vectorList.size();
		
		
		DoubleVector optimal = medoid.getCenterVector().deepCopy();
		double best = oldMedoid_DP_cost;
		for (DataPoint value : vectorList) {
			double c = distanceMeasurer.measureDistance(medoid.getCenterVector(), value.getVector());
			int flag = 0;
			int count = 1;
			for (DataPoint value2 : vectorList) {
				if (value.getVector().subtract(value2.getVector()).sum() != 0) {
					c = c + distanceMeasurer.measureDistance(value.getVector(), value2.getVector());
					flag = 1;
					count++;
				}
			}
			c = c/count;
			if (c < best && flag == 1) {
				optimal = value.getVector().deepCopy();
				best = c;
			}
		}
		
		Medoid newMedoid = new Medoid(optimal);
		centers.add(newMedoid);
		for (DataPoint vector : vectorList) {
			context.write(newMedoid, vector);
		}
		
		if (newMedoid.update(medoid))
			context.getCounter(Counter.CONVERGED).increment(1);
		
	}


	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("medoid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
				Medoid.class, IntWritable.class)) {

			//todo: serialize updated Medoid.
			final IntWritable value = new IntWritable(iteration);
			System.out.println("Iteration Print: " + iteration );
			for (Medoid center : centers) {
				try {
					out.append(center, value);
					System.out.println("CenterPrint: "+center.toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

}

