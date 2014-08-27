package com.examples.writables;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EmplSortReducer extends Reducer<EmplWritable, NullWritable, NullWritable, EmplWritable> {

	
	@Override
	protected void reduce(EmplWritable arg0, Iterable<NullWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		arg2.write(NullWritable.get(), arg0);
	}
}
