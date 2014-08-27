package com.examples.writables;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmplHavingGrpMapper extends Mapper<LongWritable, Text, IntWritable, EmplWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer s = new StringTokenizer(value.toString(), ",");
		// filter the having clause here itself sal < 3000
		
		String ename = s.nextToken();
		int sal = Integer.parseInt(s.nextToken());
		int deptNo = Integer.parseInt(s.nextToken());
		if(sal > 3000){
		EmplWritable emplWritable = new EmplWritable();
		emplWritable.setEname(new Text(ename));
	    emplWritable.setSal(new IntWritable(sal));	
		emplWritable.setDeptno(new IntWritable(deptNo));
        context.write(new IntWritable(deptNo), emplWritable);		
	}
	
}
	
}	
