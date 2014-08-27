package com.sax;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class EmplSaxHandler extends DefaultHandler {
	
	
	private SAXParser saxParser;
	private StringBuilder elementContent;
	

	public EmplSaxHandler(String input,SAXParser saxParser) throws ParserConfigurationException, SAXException, IOException {
		this.elementContent = new StringBuilder();
		this.saxParser = saxParser;
		saxParser.parse(new InputSource(new ByteArrayInputStream(input.getBytes())),this);
	}
	
	
	public String getContentString(){
		return elementContent.toString();
	}
	
	
	@Override
	public void startDocument() throws SAXException {
	 
	}
	
	@Override
	public void endDocument() throws SAXException {
	 
	}
	
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		 
	}
	
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if(! (qName.equalsIgnoreCase("dateofjoining") || qName.equalsIgnoreCase("record") 
				|| qName.equalsIgnoreCase("records")) )
		elementContent.append(",");
	}
	
	
	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		elementContent.append(ch,start,length);
	   
	}
	

}
