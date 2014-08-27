package com.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCountDriver {

	public static void main(String[] args) throws IOException {
		  
	    
	    JobConf conf = new JobConf(WordCountDriver.class);
	    conf.setJobName("Word Count ");

	    FileInputFormat.addInputPath(conf, new Path("/home/ravi/wordcount.txt"));
	    FileOutputFormat.setOutputPath(conf, new Path("/home/ravi/output/result"));
	    
	    conf.setMapperClass(WordCountMapper.class);
	    conf.setReducerClass(WordCountReducer.class);
	    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);

	    JobClient.runJob(conf);
	  }
	
}
