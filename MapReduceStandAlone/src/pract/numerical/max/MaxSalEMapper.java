package pract.numerical.max;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pract.numerical.counts.MyCounters;

public class MaxSalEMapper extends Mapper<LongWritable, Text,NullWritable, EmployeeTuple> {

	private DoubleWritable maxSal = new DoubleWritable();
	private EmployeeTuple employeeTuple = new EmployeeTuple();
	private String confparam;
	String s[];
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
	      s = context.getInputSplit().getLocations();
	      confparam = context.getConfiguration().get("test.conf");
	}
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		StringTokenizer t = new StringTokenizer(value.toString(),",");
		EmployeeTuple tuple = new EmployeeTuple();
		while(t.hasMoreTokens()){
			tuple.setEmpNo(new IntWritable(Integer.parseInt(t.nextToken())));
			tuple.seteName(new Text(t.nextToken()));
			tuple.setSal(new DoubleWritable(Double.parseDouble(t.nextToken())));
			tuple.setJoinDate(new Text(t.nextToken()));
		}
		
		// counter space
		if(tuple.getSal().get()<2000){
			context.getCounter(MyCounters.SAL_LESS_2000).increment(1);
			
		}else if(tuple.getSal().get()>2000){
			context.getCounter(MyCounters.SAL_GT_2000).increment(1);
		}
		
		System.out.println("the passed param is:"+this.confparam);
		
		
		if(this.maxSal.get() < tuple.getSal().get()){
			this.maxSal.set(tuple.getSal().get());
			this.employeeTuple.setEmpNo(tuple.getEmpNo());
			this.employeeTuple.seteName(tuple.geteName());
            this.employeeTuple.setJoinDate(tuple.getJoinDate());
            this.employeeTuple.setSal(tuple.getSal());
		}
		
	}
	
	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), employeeTuple);
	}
	
	
}
