package part4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerClass extends Reducer<PairW, IntWritable, Text, Text>{

    @Override
    protected void reduce(PairW key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        super.reduce(key, values, context);
    }
}