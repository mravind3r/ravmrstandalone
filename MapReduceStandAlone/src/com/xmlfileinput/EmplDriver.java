package com.xmlfileinput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmplDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(new Configuration(true));
		job.setJarByClass(EmplDriver.class);
		job.setMapperClass(EmplMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(MyXmlInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/empl.xml"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/xml-output"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
