package part2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<PairWritable, IntWritable, PairWritable, FloatWritable> {
	private FloatWritable result = new FloatWritable();
	private int sum;

	@Override
	public void setup(Context context) {
		sum = 0;
	}

	public void reduce(PairWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int s = 0;
		for (IntWritable i : values) {
			s += i.get();
		}
		if (key.getValue().equals("*")) {
			sum = s;
		} else {
			result.set((float) ((float) s / (float) sum));
			context.write(key, result);
		}
	}
}