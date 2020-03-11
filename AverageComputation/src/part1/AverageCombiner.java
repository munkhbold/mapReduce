package part1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageCombiner extends Reducer<Text, PairWritable, Text, PairWritable> {

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context)
            throws IOException, InterruptedException {

        super.reduce(key, values, context);
    }
}