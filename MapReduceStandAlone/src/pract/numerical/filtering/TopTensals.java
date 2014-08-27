package pract.numerical.filtering;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pract.numerical.max.EmployeeTuple;

public class TopTensals extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private TreeMap<Double, EmployeeTuple> top = new TreeMap<>();
	
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
	
		StringTokenizer t = new StringTokenizer(value.toString(),",");
		EmployeeTuple tuple = new EmployeeTuple();
		while(t.hasMoreTokens()){
			tuple.setEmpNo(new IntWritable(Integer.parseInt(t.nextToken())));
			tuple.seteName(new Text(t.nextToken()));
			tuple.setSal(new DoubleWritable(Double.parseDouble(t.nextToken())));
			tuple.setJoinDate(new Text(t.nextToken()));
		}
		
		this.top.put(tuple.getSal().get(), tuple);
		
		if(this.top.size()>10){
			this.top.remove(this.top.lastKey());
		}
		
	}
	
	
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
	
		for( Map.Entry<Double, EmployeeTuple> e :top.entrySet()){
			StringBuilder sb = new StringBuilder();
			EmployeeTuple tp = e.getValue();
			sb.append(tp.getEmpNo()).append(",");
			sb.append(tp.geteName()).append(",");
			sb.append(tp.getJoinDate()).append(",");
			sb.append(tp.getSal()).append(",");
			context.write(NullWritable.get(), sb.toString());
		}
	}
	

}
