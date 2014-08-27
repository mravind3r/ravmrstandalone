package pract.data.organization.custompartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import pract.data.organization.cvstoxml.EmplTuple;

public class CustomPartitioner extends Partitioner<IntWritable, EmplTuple>{

	@Override
	public int getPartition(IntWritable arg0, EmplTuple arg1, int numPartions) {
		
		return arg1.getDeptNo().get()% numPartions;
		
		
	}

}
