package com.word;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context){
		
		int totalCount = 0;
		
		for(IntWritable intWritable:values){
			totalCount+=intWritable.get();
		}
        try {
			context.write(key, new IntWritable(totalCount));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}
	
}
