package pract.data.organization.binning;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BinDriver {
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
		Job job = new Job(new Configuration(true));
		job.setJarByClass(BinDriver.class);
		
		
		job.setMapperClass(BinMapper.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/data/emp.dat"));
		MultipleOutputs.addNamedOutput(job, "depts",TextOutputFormat.class, 
				NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/data/bins"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
