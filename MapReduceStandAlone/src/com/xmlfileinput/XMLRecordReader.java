package com.xmlfileinput;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.xml.sax.SAXException;

import com.sax.EmplSaxHandler;

public class XMLRecordReader extends RecordReader<LongWritable, Text>{
    
	private SAXParser saxParser;
	private LongWritable key;
	private Text value;
    private LineRecordReader lineRecordReader;	
	private String endElement = "</record>";
	private String startElement = "<record>";
	
	@Override
	public void close() throws IOException {
		lineRecordReader.close();		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return lineRecordReader.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineRecordReader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(arg0, arg1);
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		try {
			this.saxParser = saxParserFactory.newSAXParser();
		} catch (ParserConfigurationException | SAXException e) {
		    throw new IOException(e.getMessage());
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String content = "";
		StringBuilder xmlRecord = new StringBuilder();
		while(lineRecordReader.nextKeyValue()){
			content = lineRecordReader.getCurrentValue().toString();
			if(!content.contains("<?xml"))
			xmlRecord.append(content.trim());
			if(content.contains(endElement)){
				break;
			}
			if(content.contains("</records>"))
				xmlRecord.setLength(0);
			
		}
		
	
		if(xmlRecord.length()!=0)	{
		EmplSaxHandler emplSaxHandler = null;
		try {
		    
			 emplSaxHandler = new EmplSaxHandler(xmlRecord.toString(), saxParser);
		} catch (ParserConfigurationException | SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
		
		
		if(emplSaxHandler.getContentString()!=null){
			this.value = new Text(emplSaxHandler.getContentString());
			return true;
		}
	}	
		
		return false;
	}

}
