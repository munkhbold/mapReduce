package part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, PairWritable, IntWritable>{
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    String[] record = value.toString().split(" ");
	    for (int i = 0; i < record.length-1; i++) {
			for (int j = i+1; j < record.length; j++) {
				if(record[i].equals(record[j]))
					break;
				context.write(new PairWritable(record[i], record[j]), new IntWritable(1));			
//				logger.info("emitting: "+record[i]+" "+record[j]+" 1");
				context.write(new PairWritable(record[i], "*"), new IntWritable(1));
//				logger.info("emitting: "+record[i]+" * 1");
			}
		}
	}
}