package part1;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, PairWritable>{
	private HashMap<String, PairWritable> buf;

	@Override
	public void setup(Context context) {
		 buf = new HashMap<>();
	}

	public boolean isCorrect(String k, String v) {
		String digitOnly = "\\d+";
	    String ipOnly = "\\d+\\.\\d+.\\d+\\.\\d+";
		return k.matches(ipOnly) && v.matches(digitOnly);
	}
	
	@Override
	public void map(Object key, Text value, Context context){
	    String[] record = value.toString().split(" ");
	    String ipAddress = record[0];
	    String v = record[record.length-1];
	    
	    if(!isCorrect(ipAddress, v))
	    	return;
	
	    PairWritable pair;
	    if(buf.containsKey(ipAddress)) {
	    	pair = buf.get(ipAddress);
	    	pair.setKey(pair.getKey() + Integer.parseInt(v));
	    	pair.setValue(pair.getValue() + 1);
	    }else {
	    	pair = new PairWritable();
	    	pair.setKey(Integer.parseInt(v));
	    	pair.setValue(1);
	    }
	
	    buf.put(ipAddress, pair);
	}
	
	private Text ip = new Text();
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(String k: buf.keySet()) {
			ip.set(k);
			context.write(ip, buf.get(k));
		}
	}
}