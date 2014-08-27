package pract.numerical.max;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TheDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new TheDriver(), args);
		System.exit(result);
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = this.getConf();
		conf.set("test.conf","10" );
		Job job = new Job(conf, "maxsal");
		job.setJarByClass(TheDriver.class);
		
		job.setMapperClass(MaxSalEMapper.class);
		job.setReducerClass(MaxSalEReducer.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(EmployeeTuple.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EmployeeTuple.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		return job.waitForCompletion(true)?0:1;
		
	}

}
