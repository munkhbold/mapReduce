package part2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduce {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average computation");
    
    job.setJarByClass(MapReduce.class);
    
    job.setMapperClass(MapperClass.class);

//    job.setCombinerClass(Combiner.class);
    
	job.setMapOutputKeyClass(PairWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(PairWritable.class);
	job.setOutputValueClass(FloatWritable.class);

	job.setReducerClass(ReducerClass.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}