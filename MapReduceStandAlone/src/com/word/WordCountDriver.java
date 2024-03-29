package com.word;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		  

	
	Job job = new Job();
	
	job.setJobName("word count");
	FileInputFormat.addInputPath(job, new Path("/home/ravi/wordcount.txt"));
	FileOutputFormat.setOutputPath(job, new Path("/home/ravi/output/result"));
	job.setMapperClass(WordMapper.class);
	job.setReducerClass(WordReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);

	
}
}
	