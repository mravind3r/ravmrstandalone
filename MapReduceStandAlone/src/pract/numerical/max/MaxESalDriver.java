package pract.numerical.max;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pract.numerical.counts.MyCounters;

public class MaxESalDriver {

	private static final Logger  LOG = Logger.getLogger(MaxESalDriver.class.getName()); 
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(new Configuration(true));
		job.setJarByClass(MaxESalDriver.class);
		
		job.setMapperClass(MaxSalEMapper.class);
		job.setReducerClass(MaxSalEReducer.class);
		
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(EmployeeTuple.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EmployeeTuple.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/ravi/emp_data.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/ravi/test3-res"));
		
		boolean resultCode = job.waitForCompletion(true);
		
		if(resultCode){
			System.out.println("the counters");
			Counters c = job.getCounters();
//			System.out.println("<2000:" + c.findCounter(MyCounters.SAL_LESS_2000).getValue());
//			System.out.println(">2000:"+c.findCounter(MyCounters.SAL_GT_2000).getValue());
//			System.out.println("rest:"+c.findCounter(MyCounters.REST).getValue());
			
			LOG.log(Level.INFO, "<2000:" + c.findCounter(MyCounters.SAL_LESS_2000).getValue());
			System.exit(0);
		}else{
			System.out.println("there was an error....");
			System.exit(1);
		}
		
		
	}
	
}
