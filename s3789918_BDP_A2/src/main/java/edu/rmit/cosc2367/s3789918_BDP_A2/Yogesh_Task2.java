package edu.rmit.cosc2367.s3789918_BDP_A2;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3789918_BDP_A2.model.WordPair;

public class Yogesh_Task2 {
	
	public static class WordPairRelativeMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private static IntWritable TotalCount = new IntWritable();
		
		// Mapper function
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Set Window size
			int WindowSize = 4;
			WordPair pair = new WordPair();
			
			String tokens[] = value.toString().split(" ");
			String word = "";
			String neighbour = "";
			
			// Check if token length is greater or equal to two so that wordPair can be formed
			if (tokens.length > 1) {
				for(int i = 0; i < tokens.length; i++) {
					word = tokens[i];
					if (word.equals("") || word==null) {
						continue;
					}
					int count = 0;
					for (int j = 1; j <= WindowSize; j++) {
						// Check neighbor to left of he word.
						if (i-j >= 0) {
							neighbour = tokens[i-j];
							if (!neighbour.equals(word)) {
								pair.setWord(word);
								pair.setNeighbour(neighbour);
								context.write(pair, ONE);
								count++;
							}
						}
						
						// Check neighbor right of the word.
						if (i+j < tokens.length) {
							neighbour = tokens[i+j];
							if (!neighbour.equals(word)) {
								pair.setWord(word);
								pair.setNeighbour(neighbour);
								context.write(pair, ONE);
								count++;
							}
						}
					}
					
					// Total number of participation of word in pairing.
					TotalCount.set(count);
					pair.setNeighbour("(");
					context.write(pair, TotalCount);
				}
			}	
			
		}
		
	}
	
	// Reducer Function
	public static class WordPairRelativeReducer extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {

		private static DoubleWritable totalCount = new DoubleWritable();
		private static DoubleWritable finalCount = new DoubleWritable();
		String Current = "Not_set_buddy";
		
		@Override
		protected void reduce(WordPair key, Iterable<IntWritable> value,
				Context context)
				throws IOException, InterruptedException {
			
			if (key.getNeighbour().equals("(")) {
				if(key.getWord().equals(Current) && !Current.equals("Not_set_buddy")) {
					int k = getCount(value);
					totalCount.set(totalCount.get()+k);
				}
				else {
					int k = getCount(value);
					totalCount.set(k);
					Current = key.getWord();
				}
			}
			else {
				int sum = getCount(value);
				finalCount.set((double) sum / totalCount.get());
				context.write(key, finalCount);				
			}
		}
		
		// Get sum of values of iterable
		public int getCount(Iterable<IntWritable> value) {
			int sum = 0;
			while (value.iterator().hasNext()) {
				sum = (int) (sum + value.iterator().next().get());
			}
			return sum;
		}
		
	}

	// Combiner Function
	public static class CombineReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
	    private IntWritable totalCount = new IntWritable();

	    @Override
	    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int count = 0;
	        for (IntWritable value : values) {
	             count += value.get();
	        }
	        totalCount.set(count);
	        context.write(key,totalCount);
	    }
	}
	
	//Partitioner function
	public static class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {

	    @Override
	    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
	        return (wordPair.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
	    }
	}
	
	//Main class
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		final Log LOG = LogFactory.getLog(Yogesh_Task2.class);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Co-occurence relative");
	
			
		job.setJarByClass(Yogesh_Task2.class);
		
		LOG.info("Mapper for task 2 configured - Yogesh Haresh Bojja, s3789918");
		job.setMapperClass(WordPairRelativeMapper.class);
		LOG.info("Combiner for task 2 configured - Yogesh Haresh Bojja, s3789918");
		job.setCombinerClass(CombineReducer.class);
		LOG.info("Partitioner for task 2 configured - Yogesh Haresh Bojja, s3789918");
		job.setPartitionerClass(WordPairPartitioner.class);
		LOG.info("Reducer for task 2 configured - Yogesh Haresh Bojja, s3789918");
		job.setReducerClass(WordPairRelativeReducer.class);
		job.setNumReduceTasks(3);
		
		job.setMapOutputKeyClass(WordPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

