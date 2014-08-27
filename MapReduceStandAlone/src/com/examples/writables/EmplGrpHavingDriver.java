package com.examples.writables;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmplGrpHavingDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(new Configuration(true));
		job.setJarByClass(EmplGrpHavingDriver.class);
		job.setMapperClass(EmplHavingGrpMapper.class);
		job.setReducerClass(EmplGrpHavingReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(EmplWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EmplWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/emp"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/having-grpby"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	
}
