package part1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageComputation {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average computation");
    
    job.setJarByClass(AverageComputation.class);
    job.setMapperClass(AverageMapper.class);

    job.setCombinerClass(AverageCombiner.class);
    job.setReducerClass(AverageReducer.class);
    
    job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);

	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}