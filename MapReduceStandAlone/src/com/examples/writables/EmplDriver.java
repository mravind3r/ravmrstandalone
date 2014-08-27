package com.examples.writables;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmplDriver {

	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
		Job job = new Job(new Configuration(true));
		job.setJarByClass(EmplDriver.class);
		job.setMapperClass(EmplSortMapper.class);
		job.setMapOutputKeyClass(EmplWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(EmplSortReducer.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/emp"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/emp-sort"));

		
		boolean status = job.waitForCompletion(true);
		System.exit(status?0:1);
		
		
   }
	
	
}
