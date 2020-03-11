package part3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperRelativeStripe extends Mapper<Object, Text, Text, MapWritable>{
	Text item = new Text();
	IntWritable value = new IntWritable();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    String[] records = value.toString().split(" ");
	    for(int i=0; i<records.length; i++) {
	    	HashMap<String, Integer> h = new HashMap<>();
	    	String s = records[i].trim();
	    	
	    	if(!s.matches("[A-Z]{1}\\d+"))
	    		continue;

	    	for(int j=i+1; j<records.length; j++) {
	    		String s2 = records[j].trim();
	    		
	    		if(!s2.matches("[A-Z]{1}\\d+"))
		    		continue;

	    		if(s.equals(s2)) break;
	    		
	    		int val = 0;
	    		if(h.containsKey(s2))
	    			val = h.get(s2);

	    		h.put(s2, val + 1);
	    	}

	    	MapWritable mapWrite = new MapWritable();
	    	for(String k: h.keySet()) {
	    		mapWrite.put(new Text(k), new IntWritable(h.get(k)));
	    	}

	    	item.set(s);
			context.write(item, mapWrite);
	    }
	}
}