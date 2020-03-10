package part1;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import java.util.Objects;

import org.apache.hadoop.io.Writable;


public class PairWritable implements Writable{
	private int key;
	private int value;
	
	public PairWritable() {
	}

	public PairWritable(int k, int v) {
		key = k;
		value = v;
	}
	
	public int getKey() {
		return key;
	}

	public int getValue() {
		return value;
	}
	
	public void setValue(int v) {
		value = v;
	}
	
	public void setKey(int k) {
		key = k;
	}
	
//	public int hashCode() {
//		return Objects.hash(key);
//	}
//	
	public String toString() {
		return "<" + key + ", " +value + ">";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readInt();
		value = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
	    out.writeInt(key);
	    out.writeInt(value);
	}

}
