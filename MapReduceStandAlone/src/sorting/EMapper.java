package sorting;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EMapper extends Mapper<LongWritable, Text, EmpRecord, EmpRecord> {

    @Override
    protected void map(LongWritable key, Text value,
    		Context context)
    		throws IOException, InterruptedException {
            Parser p = new Parser();
            EmpRecord e = p.getEmprecord(p.getParsedData(value.toString(), ","));
            context.write(e,e);
    	
    }
	
}
