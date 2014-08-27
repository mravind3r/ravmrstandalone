package com.create.records;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataGenMapper extends Mapper<Text, NullWritable, Text, NullWritable>{

	
	@Override
	protected void map(Text key, NullWritable value,
			Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
