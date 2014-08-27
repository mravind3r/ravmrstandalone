package com.word;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(new Configuration(true));
		job.setJarByClass(TestDriver.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/wordcount.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/test-res"));
	
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
}
