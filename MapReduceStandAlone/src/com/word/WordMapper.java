package com.word;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key,Text value,Context context){
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		while(tokenizer.hasMoreTokens()){
			String keyForReducer = tokenizer.nextToken();
			try {
				context.write(new Text(keyForReducer), new IntWritable(1));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
	}
	
	
}
