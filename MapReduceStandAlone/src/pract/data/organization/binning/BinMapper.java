package pract.data.organization.binning;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import pract.data.organization.cvstoxml.EmplTuple;
import pract.data.organization.cvstoxml.Utility;
import pract.numerical.max.EmployeeTuple;

public class BinMapper extends Mapper<LongWritable, Text,NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		this.multipleOutputs = new MultipleOutputs<NullWritable,Text>(context);
	}
	
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		EmplTuple tp = Utility.getEmplTuple(value.toString());
		
		if(tp.getDeptNo().get()==1){
			this.multipleOutputs.write("depts",NullWritable.get(),value,"dep-1");
		}else if(tp.getDeptNo().get()==2){
			this.multipleOutputs.write("depts", NullWritable.get(), value, "dep-2");
			
		}else if(tp.getDeptNo().get()==3){
			this.multipleOutputs.write("depts",NullWritable.get(), value, "dep-3");
		}else if(tp.getDeptNo().get()==4){
		    this.multipleOutputs.write("depts", NullWritable.get(), value, "dep-4");
		}else{
			this.multipleOutputs.write("depts", NullWritable.get(), value, "dep-5");
		}
	
		
	}
	
	
  @Override
  	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
  			throws IOException, InterruptedException {
  	  this.multipleOutputs.close();
  	}	
	

}
