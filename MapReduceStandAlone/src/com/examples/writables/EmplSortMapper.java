package com.examples.writables;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmplSortMapper  extends Mapper<LongWritable, Text, EmplWritable, NullWritable>{
	
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer s = new StringTokenizer(value.toString(),",");
		EmplWritable e = new EmplWritable();
			e.setEname(new Text(s.nextToken()));
			String sal = s.nextToken();
			e.setSal(new IntWritable(Integer.parseInt(sal)));
			e.setDeptno(new IntWritable(Integer.parseInt(s.nextToken())));
		
       
		context.write(e, NullWritable.get());
	}

}
