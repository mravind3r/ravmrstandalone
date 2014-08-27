package com.create.records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class DataGenInputFormat extends InputFormat<Text,NullWritable>{
	
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
	  System.out.println("input splits");
		List<InputSplit> splits = new ArrayList<InputSplit>();
		// say 3 splits
		for(int i=0;i<4;i++){
			splits.add(new MySplits());
		}
		System.out.println("created splits");
		return splits;
	}
	
	
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
	  MyRecordReader myRecordReader = new MyRecordReader();
	//	myRecordReader.initialize(split, context);
		return myRecordReader;
		
	}
	
	

}
