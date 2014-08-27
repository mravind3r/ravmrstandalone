package pract.data.organization.cvstoxml;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DepTuple implements WritableComparable<DepTuple>{
	
	private IntWritable deptNo;
	private Text dName;
	private IntWritable regionId;
	private Text location;
	

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.deptNo.readFields(arg0);
		this.dName.readFields(arg0);
		this.regionId.readFields(arg0);
		this.location.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
        this.deptNo.write(arg0);
        this.dName.write(arg0);
        this.regionId.write(arg0);
        this.location.write(arg0);
		
	}

	@Override
	public int compareTo(DepTuple o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public Text getdName() {
		return dName;
	}

	public void setdName(Text dName) {
		this.dName = dName;
	}

	public IntWritable getDeptNo() {
		return deptNo;
	}

	public void setDeptNo(IntWritable deptNo) {
		this.deptNo = deptNo;
	}

	public IntWritable getRegionId() {
		return regionId;
	}

	public void setRegionId(IntWritable regionId) {
		this.regionId = regionId;
	}

	public Text getLocation() {
		return location;
	}

	public void setLocation(Text location) {
		this.location = location;
	}

	@Override
	public String toString() {
		return deptNo + "," + dName
				+ "," + regionId + "," + location;
	}

}
