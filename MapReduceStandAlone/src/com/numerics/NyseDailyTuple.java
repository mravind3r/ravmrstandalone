package com.numerics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NyseDailyTuple implements Writable {
	
	private Text stock;
	private Text date;
	private FloatWritable open;
	private FloatWritable close;
	private FloatWritable high;
	private FloatWritable low;
	private IntWritable vol;
	
	
	
	public NyseDailyTuple(){
		this.stock = new Text();
		this.date = new Text();
		this.open = new FloatWritable();
		this.close = new FloatWritable();
		this.high = new FloatWritable();
		this.low = new FloatWritable();
		this.vol = new IntWritable();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	
		stock.readFields(arg0);
		date.readFields(arg0);
		open.readFields(arg0);
		close.readFields(arg0);
		high.readFields(arg0);
		low.readFields(arg0);
		vol.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		stock.write(arg0);
		date.write(arg0);
		open.write(arg0);
		close.write(arg0);
		high.write(arg0);
		low.write(arg0);
		vol.write(arg0);
	}

	@Override
	public String toString() {
		return "NyseDailyTuple [stock=" + stock + ", date=" + date + ", open="
				+ open + ", close=" + close + ", high=" + high + ", low=" + low
				+ ", vol=" + vol + "]";
	}

	public Text getStock() {
		return stock;
	}

	public void setStock(Text stock) {
		this.stock = stock;
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	public FloatWritable getOpen() {
		return open;
	}

	public void setOpen(FloatWritable open) {
		this.open = open;
	}

	public FloatWritable getClose() {
		return close;
	}

	public void setClose(FloatWritable close) {
		this.close = close;
	}

	public FloatWritable getHigh() {
		return high;
	}

	public void setHigh(FloatWritable high) {
		this.high = high;
	}

	public FloatWritable getLow() {
		return low;
	}

	public void setLow(FloatWritable low) {
		this.low = low;
	}

	public IntWritable getVol() {
		return vol;
	}

	public void setVol(IntWritable vol) {
		this.vol = vol;
	}
	
	
	

}
