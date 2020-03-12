package part3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerStripe extends Reducer<Text, MapWritable, Text, Text> {
//    private MapWritable result = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values, Context context ) throws IOException, InterruptedException{
    	HashMap<String, Integer> stripe = new HashMap<>();
    	for (MapWritable p : values) {
    		for(Writable k: p.keySet()) {
    			String s = k.toString();
    			int val = 0;
    			if(stripe.containsKey(s)) {
    				val = stripe.get(s);
    			}
    			IntWritable b = (IntWritable) p.get(k);
    			stripe.put(s, b.get() + val);
    		}
    	}

    	int sum = 0;
    	for(int v: stripe.values())
    		sum += v;

        StringBuilder result = new StringBuilder();
    	for(String k: stripe.keySet()) {
    		float f = (float)stripe.get(k) / (float)sum;
    		result.append(String.format("{%s, %.2f}", k, f));
    	}
    		
    	context.write(key, new Text(result.toString()));
    }
}