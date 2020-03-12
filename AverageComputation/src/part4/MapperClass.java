package part4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, PairW, IntWritable>{
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    String[] record = value.toString().split(" ");
	    for(int i=0; i<record.length; i++) {
	    	String s = record[i].trim();
	    	
	    	if(!s.matches("[A-Z]{1}\\d+"))
	    		continue;

	    	for(int j=i+1; j<record.length; j++) {
	    		String s2 = record[j].trim();

	    		if(!s2.matches("[A-Z]{1}\\d+"))
		    		continue;
	    		else if( s == "" || s2 == "")
	    			continue;
	    		else if(s.equals(s2)) break;

				context.write(new PairW(s, s2), new IntWritable(1));
	    	}
	    }
	}
}