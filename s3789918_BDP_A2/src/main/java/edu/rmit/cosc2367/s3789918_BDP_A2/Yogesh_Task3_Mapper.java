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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.rmit.cosc2367.s3789918_BDP_A2.distance.DistanceMeasurer;
import edu.rmit.cosc2367.s3789918_BDP_A2.distance.EuclidianDistance;
import edu.rmit.cosc2367.s3789918_BDP_A2.model.DataPoint;
import edu.rmit.cosc2367.s3789918_BDP_A2.model.Medoid;

// Mapper function
public class Yogesh_Task3_Mapper extends Mapper<Medoid, DataPoint, Medoid, DataPoint>{
	private final List<Medoid> centers = new ArrayList<>();
	private DistanceMeasurer distanceMeasurer;
	private static final Log LOG = LogFactory.getLog(Yogesh_Task3_Mapper.class);
	
	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		Path medoids = new Path(conf.get("medoid.path"));
		FileSystem fs = FileSystem.get(conf);

		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, medoids, conf)) {
			Medoid key = new Medoid();
			IntWritable value = new IntWritable();
			int index = 0;
			while (reader.next(key, value)) {
				Medoid medoid = new Medoid(key);
				medoid.setClusterIndex(index++);
				centers.add(medoid);
			}
		}

		distanceMeasurer = new EuclidianDistance();
		
	}


	@Override
	protected void map(Medoid medoid, DataPoint dataPoint, Context context) throws IOException,
			InterruptedException {

		Medoid nearest = null;
		double nearestDistance = Double.MAX_VALUE;

		for (Medoid c : centers) {
			//todo: find the nearest centroid for the current dataPoint, pass the pair to reducer
			double dist = distanceMeasurer.measureDistance(c.getCenterVector(), dataPoint.getVector());
			if (dist < nearestDistance || nearest == null) {
				nearestDistance = dist;
				nearest = c;
			}
		}

		context.write(nearest, dataPoint);
		
	}

}
