package com.numerics;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// find the max volume traded for a particular stock and display the record
// store unique records for every stock


public class MaxVolumeTradedRecordMapper extends Mapper<LongWritable, Text, NullWritable, NyseDailyTuple>{

	private HashMap<String,NyseDailyTuple> dailyTuples;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		dailyTuples = new HashMap<String,NyseDailyTuple>();
	}
	
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
      
		StringTokenizer s = new StringTokenizer(value.toString());
		
		NyseDailyTuple n = new NyseDailyTuple();
		s.nextToken();
		String stock = s.nextToken();
		n.setStock(new Text(stock));
		n.setDate(new Text(s.nextToken()));
		n.setOpen(new FloatWritable(Float.parseFloat(s.nextToken())));
		n.setClose(new FloatWritable(Float.parseFloat(s.nextToken())));
		n.setHigh(new FloatWritable(Float.parseFloat(s.nextToken())));
		n.setLow(new FloatWritable(Float.parseFloat(s.nextToken())));
		n.setVol(new IntWritable(Integer.parseInt(s.nextToken())));
		
		
		NyseDailyTuple maxVolTuple = dailyTuples.get(stock);
		
		if(maxVolTuple!=null){
			int volume = maxVolTuple.getVol().get();
			if(n.getVol().get() > volume){
				dailyTuples.put(stock, n);
			}
		}else{
			dailyTuples.put(stock, n);
		}
		
	}
	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		for(String s: dailyTuples.keySet()){
			context.write(NullWritable.get(), dailyTuples.get(s));
		}
	}
	
	
}
