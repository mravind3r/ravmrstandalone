package sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(new Configuration(true));
		job.setJarByClass(EDriver.class);
		
		
		job.setPartitionerClass(EmpPartitioner.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setGroupingComparatorClass(GroupByEmployeeSalAsc.class);
     		
		job.setMapperClass(EMapper.class);
		job.setMapOutputKeyClass(EmpRecord.class);
		job.setMapOutputValueClass(EmpRecord.class);
		
		job.setReducerClass(EReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EmpRecord.class);
		
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/emp_data.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/emp_sort"));
		
		
		
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	
}
