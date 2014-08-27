package pract.numerical.max;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxSalEReducer extends Reducer<NullWritable, EmployeeTuple, NullWritable,EmployeeTuple>{
	
	
	private DoubleWritable maxsal = new DoubleWritable();
	private EmployeeTuple empTuple = new EmployeeTuple();

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	    context.write(NullWritable.get(), this.empTuple);
	}
	
	
	@Override
	protected void reduce(NullWritable arg0, Iterable<EmployeeTuple> arg1,
			Context arg2)
			throws IOException, InterruptedException {
	
		for(EmployeeTuple e:arg1){
			
			if(this.maxsal.get() < e.getSal().get()){
				this.maxsal.set(e.getSal().get());
				this.empTuple.setEmpNo(e.getEmpNo());
				this.empTuple.seteName(e.geteName());
				this.empTuple.setJoinDate(e.getJoinDate());
				this.empTuple.setSal(e.getSal());
			}
		}
		
	}
	
	//@Override
	protected void reduce(DoubleWritable arg0, Iterable<EmployeeTuple> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		
		if(arg0.get()  >  this.maxsal.get()){
			this.maxsal.set(arg0.get());
			for(EmployeeTuple t :arg1){
				this.empTuple.setEmpNo(t.getEmpNo());
				this.empTuple.seteName(t.geteName());
				this.empTuple.setJoinDate(t.getJoinDate());
				this.empTuple.setSal(t.getSal());	
			}
			
		}
		
	}
	
	
	
	
}
