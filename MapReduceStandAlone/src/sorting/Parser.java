package sorting;

import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Parser {

	public String[] getParsedData(String input,String regex){
	   	Pattern p = Pattern.compile(regex);
	   	return p.split(input);
	}
	
	
	public EmpRecord getEmprecord(String[] data){
		EmpRecord e = new EmpRecord();
		e.setEmpNo(new IntWritable(Integer.parseInt(data[0])));
		e.setName(new Text(data[1]));
		e.setSal(new DoubleWritable(Double.parseDouble(data[2])));
		e.setDateofJoining(new Text(data[3]));
		
		return e;
	}
	
}
