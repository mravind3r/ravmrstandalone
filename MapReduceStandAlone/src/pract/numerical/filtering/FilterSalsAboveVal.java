package pract.numerical.filtering;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pract.numerical.max.EmployeeTuple;

public class FilterSalsAboveVal extends Mapper<LongWritable, Text,NullWritable, Text>{

	private int count =0;
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		count++;
		StringTokenizer t = new StringTokenizer(value.toString(),",");
		while(t.hasMoreTokens()){
			t.nextToken();
			t.nextToken();
			if( Double.parseDouble(t.nextToken()) > 5000 ) {
				context.write(NullWritable.get(), value);
			}
			t.nextToken();
			   
		}
	}
	
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		System.out.println("num of records:" + this.count);
	}
	
	
	
}
