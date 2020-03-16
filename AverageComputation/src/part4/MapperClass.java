package part4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import part2.PairWritable;

public class MapperClass extends Mapper<Object, Text, PairWritable, IntWritable> {

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
	}
}
