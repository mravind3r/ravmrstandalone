package pract.data.organization.cvstoxml;


import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, Text,Text> {
	
    private Text outkey = new Text();
    private Text outValue = new Text();
	
	  @Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		 
		EmplTuple e = Utility.getEmplTuple(value.toString());  
		
		outkey.set(String.valueOf(e.getDeptNo().get()));
		outValue.set("E"+ e.toString());
        
		context.write(outkey, outValue);
		
		  
		  
		  
	}
	

}
