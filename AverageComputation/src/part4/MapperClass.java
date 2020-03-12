package part4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import part2.PairWritable;

public class MapperClass extends Mapper<Object, Text, PairWritable, IntWritable> {

//	PairW pair = new PairW();
//	IntWritable one = new IntWritable(1);
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] record = value.toString().split(" ");
		for (int i = 0; i < record.length - 1; i++) {
			for (int j = i + 1; j < record.length; j++) {
				if (record[i].equals(record[j]))
					break;
				context.write(new PairWritable(record[i], record[j]), new IntWritable(1));
			}
		}
		// for(int i=0; i<record.length; i++) {
//	    	String s = record[i].trim();
//
//	    	if(!s.matches("[A-Z]{1}\\d+"))
//	    		continue;
//
//	    	for(int j=i+1; j<record.length; j++) {
//	    		String s2 = record[j].trim();
//
//	    		if(!s2.matches("[A-Z]{1}\\d+"))
//		    		continue;
//
//	    		else if(s.equals(s2)) break;
//
//	    		pair.setKey(s);
//	    		pair.setValue(s2);
//				context.write(pair, one);
//	    	}
//	    }
	}
}