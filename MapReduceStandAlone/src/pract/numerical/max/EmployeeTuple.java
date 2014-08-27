package pract.numerical.max;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class EmployeeTuple implements WritableComparable<EmployeeTuple>{
	
	@Override
	public String toString() {
		return "EmployeeTuple [empNo=" + empNo + ", eName=" + eName
				+ ", joinDate=" + joinDate + ", sal=" + sal + "]";
	}


	public IntWritable getEmpNo() {
		return empNo;
	}


	public void setEmpNo(IntWritable empNo) {
		this.empNo = empNo;
	}


	public Text geteName() {
		return eName;
	}


	public void seteName(Text eName) {
		this.eName = eName;
	}


	public Text getJoinDate() {
		return joinDate;
	}


	public void setJoinDate(Text joinDate) {
		this.joinDate = joinDate;
	}


	public DoubleWritable getSal() {
		return sal;
	}


	public void setSal(DoubleWritable sal) {
		this.sal = sal;
	}

	private IntWritable empNo;
	private Text eName;
	private Text joinDate;
	private DoubleWritable sal;
	
	public EmployeeTuple(){
		this.empNo = new IntWritable();
		this.eName = new Text();
		this.joinDate = new Text();
		this.sal = new DoubleWritable();
	}
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.empNo.readFields(arg0);
		this.eName.readFields(arg0);
		this.sal.readFields(arg0);
		this.joinDate.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.empNo.write(arg0);
		this.eName.write(arg0);
		this.sal.write(arg0);
		this.joinDate.write(arg0);
		
	}

	@Override
	public int compareTo(EmployeeTuple o) {
		return this.eName.toString().compareTo(o.toString());
	}

}
