package pract.data.organization.cvstoxml;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeptMapper extends Mapper<LongWritable, Text,Text, Text> {
	
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
	
		 DepTuple d = Utility.getDepTuple(value.toString());
         
		 outKey.set(String.valueOf(d.getDeptNo().get()));
		 outValue.set("D"+ d.toString());
		 
		 context.write(outKey, outValue);
		
	}

}
