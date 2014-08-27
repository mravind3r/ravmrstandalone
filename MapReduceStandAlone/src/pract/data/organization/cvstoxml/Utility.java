package pract.data.organization.cvstoxml;

import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Utility {
	
	public static EmplTuple getEmplTuple(String data){
		
		 StringTokenizer t = new StringTokenizer(data,",");
		  EmplTuple e = new EmplTuple();
		  while(t.hasMoreTokens()){
			  e.setEmpNo(new IntWritable(Integer.parseInt(t.nextToken())));
			  e.setEname(new Text(t.nextToken()));
			  e.setSal(new DoubleWritable(Double.parseDouble(t.nextToken())));
			  int deptNo = Integer.parseInt(t.nextToken());
			  e.setDeptNo(new IntWritable(deptNo));
		  }
		  
		  
		  return e;
	}
	

	
	public static DepTuple getDepTuple(String data){
		StringTokenizer t = new StringTokenizer(data,",");
		DepTuple d = new DepTuple();
		while(t.hasMoreTokens()){
			int deptNo = Integer.parseInt(t.nextToken());
			d.setDeptNo(new IntWritable(deptNo));
			d.setdName(new Text(t.nextToken()));
			d.setRegionId(new IntWritable(Integer.parseInt(t.nextToken())));
			d.setLocation(new Text(t.nextToken()));
		}
		
		return d;
	}
	
	
}
