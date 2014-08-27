package pract.data.organization.cvstoxml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StructuredToHierarchialDriver {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(new Configuration(true));
		job.setJarByClass(StructuredToHierarchialDriver.class);
		
		MultipleInputs.addInputPath(job, new Path("/home/ravi/data/emp.dat"),
				                   TextInputFormat.class,EmpMapper.class);
		MultipleInputs.addInputPath(job, new Path("/home/ravi/data/dept.dat"),
                TextInputFormat.class,DeptMapper.class);
	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(EmpDeptReducer.class);
		
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/data/res1"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
}
