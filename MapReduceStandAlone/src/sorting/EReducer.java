package sorting;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class EReducer extends Reducer<EmpRecord,EmpRecord,NullWritable,EmpRecord> {


	@Override
	protected void reduce(EmpRecord arg0, Iterable<EmpRecord> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		for(EmpRecord e:arg1){
			System.out.println(e.getEmpNo() + " " + e.getName());
			arg2.write(NullWritable.get(), e);	
		}
		
	}
	
	
}
