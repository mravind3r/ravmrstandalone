package pract.data.organization.cvstoxml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class EmplTuple implements WritableComparable<EmplTuple>{
	
	private IntWritable empNo;
	private Text ename;
	private DoubleWritable sal;
	private IntWritable deptNo;
	
	

	public IntWritable getEmpNo() {
		return empNo;
	}

	public void setEmpNo(IntWritable empNo) {
		this.empNo = empNo;
	}

	public Text getEname() {
		return ename;
	}

	public void setEname(Text ename) {
		this.ename = ename;
	}

	public DoubleWritable getSal() {
		return sal;
	}

	public void setSal(DoubleWritable sal) {
		this.sal = sal;
	}

	public IntWritable getDeptNo() {
		return deptNo;
	}

	public void setDeptNo(IntWritable deptNo) {
		this.deptNo = deptNo;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.empNo.readFields(arg0);
		this.ename.readFields(arg0);
		this.sal.readFields(arg0);
		this.deptNo.readFields(arg0);
	
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.empNo.write(arg0);
		this.ename.write(arg0);
		this.sal.write(arg0);
		this.deptNo.write(arg0);
		
	}

	@Override
	public int compareTo(EmplTuple o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return empNo + "," + ename + ","
				+ sal + "," + deptNo ;
	}

}
