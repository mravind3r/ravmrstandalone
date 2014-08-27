package com.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		
		String line = value.toString();
		StringTokenizer stringTokenizer = new StringTokenizer(line);
		int counter = 0;
		while(stringTokenizer.hasMoreTokens()){
			//counter++;
			output.collect(new Text(stringTokenizer.nextToken()), new IntWritable(1));
		}
		
		//output.collect(new Text("total words"), new IntWritable(counter));
		
		
		
	}

}
