package part4;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import part2.PairWritable;

public class ReducerClass extends Reducer<PairWritable, IntWritable, Text, Text> {
	HashMap<String, Integer> buf;
	String uprev;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		buf = new HashMap<>();
		uprev = null;
		super.setup(context);
	}

	private int getSum() {
		int sum = 0;
		for (int i : buf.values()) 
			sum += i;
		return sum;
	}

	public void reduce(PairWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String u = key.getKey();
		String v = key.getValue();
		if (!u.equals(uprev) && uprev != null) {
			int sum = getSum();
			StringBuilder result = new StringBuilder();
			for (String k : buf.keySet()) {
				float f = (float) buf.get(k) / (float) sum;
				result.append(String.format("{%s, %.2f}", k, f));
			}
			context.write(new Text(uprev), new Text(result.toString()));
			buf = new HashMap<>();
		}

		uprev = u;
		int total = 0;
		for (IntWritable i : values) {
			total += i.get();
		}
		buf.put(v, total);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		int sum = getSum();

		StringBuilder result = new StringBuilder();
		for (String k : buf.keySet()) {
			float f = (float) buf.get(k) / (float) sum;
			result.append(String.format("{%s, %.2f}", k, f));
		}
		context.write(new Text(uprev), new Text(result.toString()));
		super.cleanup(context);
	}
}