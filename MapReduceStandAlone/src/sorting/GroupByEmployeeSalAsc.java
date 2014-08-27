package sorting;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupByEmployeeSalAsc extends WritableComparator {

	protected GroupByEmployeeSalAsc() {
		super(EmpRecord.class, true);
	}

	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		EmpRecord first = (EmpRecord) a;
		EmpRecord second = (EmpRecord) b;
		
		int firstSal = first.getEmpNo().get();
		int secondSal = second.getEmpNo().get();
		
		if(firstSal>secondSal){
			return 1;
		}else if(firstSal<secondSal){
			return -1;
		}
		
		return 0;
	}
	
	

}
