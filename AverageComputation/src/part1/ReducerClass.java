package part1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, PairWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<PairWritable> values, Context context ) throws IOException, InterruptedException{
      int sum = 0;
      int qty = 0;
      for (PairWritable p : values) {
    	  qty += p.getValue();
    	  sum += p.getKey();
      }
//      result.setKey(sum);
//      result.setValue(qty);
      result.set((float) ((float)sum/(float)qty));
      context.write(key, result);
    }
  }