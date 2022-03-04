package edu.rmit.cosc2367.s3789918_BDP_A2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.rmit.cosc2367.s3789918_BDP_A2.model.DataPoint;
import edu.rmit.cosc2367.s3789918_BDP_A2.model.Medoid;


public class Yogesh_Task3 {

	private static final Log LOG = LogFactory.getLog(Yogesh_Task3.class);
	private static ArrayList<String> data = new ArrayList();

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		

		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");
		
		Path PointDataPath = new Path("clustering/data.seq");
		Path medoidDataPath = new Path("clustering/medoid.seq");
		conf.set("medoid.path", medoidDataPath.toString());
		Path outputDir = new Path("clustering/depth_1");

		Job job = Job.getInstance(conf);
		job.setJobName("KMedoid Clustering");

		job.setMapperClass(Yogesh_Task3_Mapper.class);
		job.setReducerClass(Yogesh_Task3_Reducer.class);
		job.setJarByClass(Yogesh_Task3_Mapper.class);

		FileInputFormat.addInputPath(job, PointDataPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		if (fs.exists(medoidDataPath)) {
			fs.delete(medoidDataPath, true);
		}

		if (fs.exists(PointDataPath)) {
			fs.delete(PointDataPath, true);
		}

		data = readFile(conf, args[0]);
		generateMedoid(conf, medoidDataPath, fs, args[1]);
		generateDataPoints(conf, PointDataPath, fs, args[1]);

		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Medoid.class);
		job.setOutputValueClass(DataPoint.class);

		job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(Yogesh_Task3_Reducer.Counter.CONVERGED).getValue();
		LOG.info("Number of Iteration : "+iteration);
		iteration++;
		
		while (counter > 0) {
			conf = new Configuration();
			conf.set("medoid.path", medoidDataPath.toString());
			conf.set("num.iteration", iteration + "");
			job = Job.getInstance(conf);
			job.setJobName("KMedoid Clustering " + iteration);

			job.setMapperClass(Yogesh_Task3_Mapper.class);
			job.setReducerClass(Yogesh_Task3_Reducer.class);
			job.setJarByClass(Yogesh_Task3_Mapper.class);

			PointDataPath = new Path("clustering/depth_" + (iteration - 1) + "/");
			outputDir = new Path("clustering/depth_" + iteration);

			FileInputFormat.addInputPath(job, PointDataPath);
			if (fs.exists(outputDir))
				fs.delete(outputDir, true);

			FileOutputFormat.setOutputPath(job, outputDir);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Medoid.class);
			job.setOutputValueClass(DataPoint.class);
			job.setNumReduceTasks(1);

			job.waitForCompletion(true);
			LOG.info("Number of Iteration : "+iteration);
			iteration++;

			counter = job.getCounters().findCounter(Yogesh_Task3_Reducer.Counter.CONVERGED).getValue();
		}

		Path result = new Path("clustering/depth_" + (iteration - 1) + "/");
		FileSystem fswriter = FileSystem.get(conf);
		BufferedWriter bfwriter = new BufferedWriter(new OutputStreamWriter(fswriter.create(new Path(args[2]), true)));
		FileStatus[] stati = fs.listStatus(result);
		for (FileStatus status : stati) {
			if (!status.isDir()) {
				Path path = status.getPath();
				if (!path.getName().equals("_SUCCESS")) {
					LOG.info("FOUND " + path.toString());
					try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
						Medoid key = new Medoid();
						DataPoint v = new DataPoint();
						while (reader.next(key, v)) {
							LOG.info(key + " / " + v);
							bfwriter.write(key + " / " + v +"\n");
						}
					}
				}
			}
		}
		bfwriter.close();
		fswriter.close();	
		
	}

	// Read datapoints from list and store in data.seq
	@SuppressWarnings("deprecation")
	public static void generateDataPoints(Configuration conf, Path in, FileSystem fs, String k) throws IOException {	
		try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Medoid.class,
				DataPoint.class)) {
			int j = 0;
			int numOfK = Integer.parseInt(k);
			numOfK = numOfK*100+1;
			for (String d : data) {
				j++;  
                String[] s = d.split(" ");
                double point1 = Double.parseDouble(s[0]);
                double point2 = Double.parseDouble(s[1]);
              
                dataWriter.append(new Medoid(new DataPoint(0, 0)), new DataPoint(point1, point2));
                
			}
			dataWriter.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

	//Extract medoids and store it in centroid.seq
	@SuppressWarnings("deprecation")
	public static void generateMedoid(Configuration conf, Path center, FileSystem fs, String k) throws IOException {
		int numOfK = Integer.parseInt(k);
		double[] d = {-73.981636047363281, 40.732769012451172, -73.987045288085938, 40.760906219482422, -73.979301452636719, 40.767261505126953, 
    			-73.964279174804688, 40.768009185791016, -73.984382629394531, 40.745979309082031, -73.980377197265625, 40.777301788330078};
		try {
			SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Medoid.class, IntWritable.class);
			final IntWritable value = new IntWritable(0);
			
	    	for(int i = 0; i<numOfK*2; i=i+2) {
	    		centerWriter.append(new Medoid(new DataPoint(d[i], d[i+1])), value);
	    	}
	    	centerWriter.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

	//Read points from NYTaxiLC1000 and store it in list
	public static ArrayList<String> readFile(Configuration conf, String input){
		FileSystem f;
		ArrayList<String> arr = new ArrayList();
		try {
			f = FileSystem.get(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(f.open(new Path(input))));
			String data = "";
			while((data=reader.readLine()) != null) {
				arr.add(data);
			}
			reader.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		return arr;
	}
}

