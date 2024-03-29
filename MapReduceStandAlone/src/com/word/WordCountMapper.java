package com.word;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		String input = value.toString();
		StringTokenizer st = new StringTokenizer(input);
		while(st.hasMoreTokens()){
			String tok = st.nextToken();
			context.write(new Text(tok), new IntWritable(1));
		}
		
		
	}

}
