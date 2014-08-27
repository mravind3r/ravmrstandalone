package com.create.records;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyRecordReader extends RecordReader<Text, NullWritable> {

	private Text key;
	private int recordsPerSplit = 0;
	
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		recordsPerSplit =0;
		System.out.println("called once inside initialize myrecord reader");
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new Text();
		if(recordsPerSplit<50){
		   key.set("Record:"+recordsPerSplit);	
		   recordsPerSplit++;
			return true;
		}
		
		return false;
	}

}
