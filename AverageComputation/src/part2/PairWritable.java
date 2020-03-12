package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable> {
	private String key;
	private String value;

	public PairWritable() {

	}

	public PairWritable(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setKey(String key) {
		this.key = key;
	}


	public String toString() {
		return "<" + key + ", " + value + ">";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.key = arg0.readUTF();
		this.value = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(key);
		arg0.writeUTF(value);
	}

	@Override
	public int compareTo(PairWritable o) {
		// TODO Auto-generated method stub
		int k = this.key.compareTo(o.key);
		if(k!=0) return k;
		return this.value.compareTo(o.value);
	}

}
