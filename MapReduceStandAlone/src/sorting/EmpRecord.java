package sorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class EmpRecord implements WritableComparable<EmpRecord> {

	private IntWritable empNo;
	private Text name;
	private DoubleWritable sal;
	private Text dateofJoining;
	
	public EmpRecord(){
		this.empNo = new IntWritable();
		this.name = new Text();
		this.sal = new DoubleWritable();
		this.dateofJoining = new Text();
	}
	
	
	
	
	
	@Override
	public String toString() {
		return "EmpRecord [empNo=" + empNo + ", name=" + name + ", sal=" + sal
				+ ", dateofJoining=" + dateofJoining + "]";
	}





	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.empNo.readFields(arg0);
		this.name.readFields(arg0);
		this.sal.readFields(arg0);
		this.dateofJoining.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.empNo.write(arg0);
		this.name.write(arg0);
		this.sal.write(arg0);
		this.dateofJoining.write(arg0);
		
		
	}

	@Override
	public int compareTo(EmpRecord o) {
		return this.name.compareTo(o.name);
	}

	public Text getDateofJoining() {
		return dateofJoining;
	}

	public void setDateofJoining(Text dateofJoining) {
		this.dateofJoining = dateofJoining;
	}

	public DoubleWritable getSal() {
		return sal;
	}

	public void setSal(DoubleWritable sal) {
		this.sal = sal;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public IntWritable getEmpNo() {
		return empNo;
	}

	public void setEmpNo(IntWritable empNo) {
		this.empNo = empNo;
	}

	
	
	
}
