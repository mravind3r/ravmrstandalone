package com.create.records;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 
 some basics 
 1)InputFormat has got a method called getSplits that returns an array of splits
 Leverage this and return as many splits you want , cos each split = 1 map task
 override the getSplits method()
 2) Over ride record reader 
 As we saw before we use the LineRecordreader class and make use of the RecordReader over ride methods
 in the method getNextKeyValue()
 rather than linerecord reader .nextKeyvalue determining what record is to be returned
 we generate the record here. and set to true and false .
 we populate the key here and not the valyue , we will return value as nullwritable
 
 3) since we do not know the size of the split and the number of records that have to be in each split , we 
 loop here to return the records based on configuration
 
 
 
 */



public class DataDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(new Configuration(true));
		job.setJarByClass(DataDriver.class);
		job.setMapperClass(DataGenMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(DataGenInputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/home/ravi/data-gen"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
}
