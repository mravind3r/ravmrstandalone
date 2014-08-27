package pract.data.organization.cvstoxml;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpDeptReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text outValue = new Text();
	private static String s = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";

	private JAXBContext jaxbContext;

	private Marshaller marshaller;

	private List<String> deptString = new ArrayList<String>();

	private List<String> eString = new ArrayList<String>();

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {

		try {
			jaxbContext = JAXBContext.newInstance(Employee.class);
			marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
					Boolean.TRUE);
			
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		StringWriter stringWriter = null;
		for (Text t : arg1) {
			if (t.toString().charAt(0) == 'D') {
				deptString.add(t.toString().replaceAll("D", ""));
			} else {
				eString.add(t.toString().replaceAll("E", ""));
			}
		}

		if (deptString.size() > 0 && eString.size() > 0) {

			for (String d : deptString) {
				DepTuple dTuple = Utility.getDepTuple(d);
				for (String e : eString) {
					EmplTuple eTuple = Utility.getEmplTuple(e);
					stringWriter = new StringWriter();
					Employee employee = new Employee();
					employee.setEmpNo(eTuple.getEmpNo().get());
					employee.setDeptNo(dTuple.getDeptNo().get());
					employee.setdName(dTuple.getdName().toString());
					employee.seteName(eTuple.getEname().toString());
					employee.setSal(eTuple.getSal().get());
					employee.setLocation(dTuple.getLocation().toString());

					try {
						marshaller.marshal(employee, stringWriter);
					} catch (JAXBException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

				}

			}

		}
		
		String output = new String(stringWriter.toString());
		System.out.println(output);
		
		
		outValue.set(output);
		arg2.write(NullWritable.get(),outValue);

	}

}
