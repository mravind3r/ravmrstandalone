package sorting;

import org.apache.hadoop.mapreduce.Partitioner;

public class EmpPartitioner extends Partitioner<EmpRecord, EmpRecord> {

	@Override
	public int getPartition(EmpRecord arg0, EmpRecord arg1, int arg2) {
		
		return Math.abs(arg0.getName().hashCode()*3)%arg2;
		
	}

}
