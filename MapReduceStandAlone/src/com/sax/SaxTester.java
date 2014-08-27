package com.sax;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

public class SaxTester {

	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
		
		String input = "<record><ename>Jin Z. Lowe</ename><sal>4884</sal>"+
						"<address>4565 Vulputate Av.</address><dateofjoining>03/02/14</dateofjoining>"+
						"</record>";
		String x = "<record><ename>Maxine W. Cole</ename><sal>3545</sal>"
				+ "<address>Ap #909-927 Nunc Ave</address><dateofjoining>07/05/15"
				+ "</dateofjoining></record>";	
		
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		SAXParser saxParser = saxParserFactory.newSAXParser();
		EmplSaxHandler emplHandler = new EmplSaxHandler(x,saxParser);
		System.out.println(emplHandler.getContentString());
		
		String s = "<record></records>";
		System.out.println(s.contains("</record>"));
		
		
	}
	
}
