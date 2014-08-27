package com.examples.writables;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EmplGrpHavingReducer extends Reducer<IntWritable, EmplWritable, NullWritable, EmplWritable> {

	 @Override
	protected void reduce(IntWritable arg0, Iterable<EmplWritable> arg1,
	             Context arg2)
			throws IOException, InterruptedException {
		for(EmplWritable e :arg1){
			arg2.write(NullWritable.get(), e);
		}
	}
	
	
}
