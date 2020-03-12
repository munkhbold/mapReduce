package part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class PairW implements WritableComparable<PairW>{
	private String key;
	private String value;
	
	public PairW(String k, String v) {
		key = k;
		value = v;
	}
	
	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}
	
	public void setValue(String v) {
		value = v;
	}
	
	public void setKey(String k) {
		key = k;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		value = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
	    out.writeUTF(key);
	    out.writeUTF(value);
	}

	@Override
	public int compareTo(PairW o) {
		return key.compareTo(o.getKey());
	}
}
