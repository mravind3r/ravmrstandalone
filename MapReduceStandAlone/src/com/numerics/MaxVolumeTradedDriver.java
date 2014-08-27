package com.numerics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxVolumeTradedDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(new Configuration(true));
		job.setJarByClass(MaxVolumeTradedDriver.class);
		job.setMapperClass(MaxVolumeTradedRecordMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NyseDailyTuple.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/NYSE_daily"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/max-vol-data"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
