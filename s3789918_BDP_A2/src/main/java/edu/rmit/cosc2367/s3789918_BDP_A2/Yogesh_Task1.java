package edu.rmit.cosc2367.s3789918_BDP_A2;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3789918_BDP_A2.model.WordPair;


public class Yogesh_Task1 {
	
	public static class WordPairMapper  extends Mapper<LongWritable, Text, WordPair, LongWritable> {
		private final static LongWritable ONE = new LongWritable(1);
		private static final Logger LOG = Logger.getLogger(WordPairMapper.class);
		
		// Mapper Function
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WordPair, LongWritable>.Context context)
				throws IOException, InterruptedException {
			LOG.setLevel(Level.INFO);
			LOG.info("Mapper for task 1 running - Yogesh Haresh Bojja, s3789918");
			
			// Set Window size
			int WindowSize = 4;
			WordPair pair = new WordPair();
			
			String tokens[] = value.toString().split(" ");
			String word = "";
			String neighbour = "";
			
			for(int i = 0; i < tokens.length; i++) {
				word = tokens[i];
				for (int j = 1; j <= WindowSize; j++) {
					// Check neighbor left to the word
					if (i-j >= 0) {
						neighbour = tokens[i-j];
						// Check if word is not same as neighbor
						if (!neighbour.equals(word)) {
							pair.setWord(word);
							pair.setNeighbour(neighbour);
							context.write(pair, ONE);
						}
					}
					// Check neighbor right to the word
					if (i+j < tokens.length) {
						neighbour = tokens[i+j];
						// Check if word is not same as neighbor
						if (!neighbour.equals(word)) {
							pair.setWord(word);
							pair.setNeighbour(neighbour);
							context.write(pair, ONE);
						}
					}
				}
			}
			
			
		}
		
	}
	
	// Reduce function
	public static class WordPairReducer extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
		private static final Logger LOG = Logger.getLogger(WordPairReducer.class);
		@Override
		protected void reduce(WordPair key, Iterable<LongWritable> value,
				Context context)
				throws IOException, InterruptedException {
			LOG.setLevel(Level.INFO);
			LOG.info("Reducer for task 1 running - Yogesh Haresh Bojja, s3789918");
			
			long sum = 0;
			while (value.iterator().hasNext()) {
				sum = sum + value.iterator().next().get();
			}
			context.write(key, new LongWritable(sum));
		}
		
	}

	// Main function
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		final Log LOG = LogFactory.getLog(Yogesh_Task1.class);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Co-occurence");
	
			
		job.setJarByClass(Yogesh_Task1.class);
		
		LOG.info("Mapper for task 1 configured - Yogesh Haresh Bojja, s3789918");
		job.setMapperClass(WordPairMapper.class);
		LOG.info("Combiner for task 1 configured - Yogesh Haresh Bojja, s3789918");
		job.setCombinerClass(WordPairReducer.class);
		LOG.info("Reducer for task 1 configured - Yogesh Haresh Bojja, s3789918");
		job.setReducerClass(WordPairReducer.class);
		
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

