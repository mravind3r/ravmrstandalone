package com.examples.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class EmplWritable implements WritableComparable<EmplWritable>{

	private Text ename;
	private IntWritable sal;
	private IntWritable deptno;
	
	public EmplWritable(){
		this.ename = new Text();
		this.sal = new IntWritable();
		this.deptno = new IntWritable();
	}
	
	public Text getEname() {
		return ename;
	}

	public void setEname(Text ename) {
		this.ename = ename;
	}

	public IntWritable getSal() {
		return sal;
	}

	public void setSal(IntWritable sal) {
		this.sal = sal;
	}

	public IntWritable getDeptno() {
		return deptno;
	}

	public void setDeptno(IntWritable deptno) {
		this.deptno = deptno;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.ename.readFields(arg0);
		this.sal.readFields(arg0);
		this.deptno.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.ename.write(arg0);
		this.sal.write(arg0);
		this.deptno.write(arg0);
		
	}

	
	//1) order the keys by name
	
	@Override
	public int compareTo(EmplWritable o) {
		return this.ename.compareTo(o.getEname());
	}

	@Override
	public String toString() {
		return  ename + ","+ sal + ","
				+ deptno ;
	}
	
	

}
