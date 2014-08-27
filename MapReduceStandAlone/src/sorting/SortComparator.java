package sorting;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

	protected SortComparator() {
	   super(EmpRecord.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		EmpRecord f = (EmpRecord)a;
		EmpRecord s = (EmpRecord)b;
		
		
		return f.getName().toString().compareTo(s.getName().toString());
	}
	
}
