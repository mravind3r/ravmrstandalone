package pract.numerical.filtering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterSalAboveDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(new Configuration(true));
		
		job.setJarByClass(FilterSalAboveDriver.class);
		job.setNumReduceTasks(0);
		
		job.setMapperClass(FilterSalsAboveVal.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/emp_data.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/test2-res"));
		
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
}
