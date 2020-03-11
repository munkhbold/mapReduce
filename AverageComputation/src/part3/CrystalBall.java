package part3;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CrystalBall {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Crystal ball");
    
    job.setJarByClass(CrystalBall.class);
    job.setMapperClass(MapperRelativeStripe.class);

    job.setCombinerClass(CombinerStripe.class);
    job.setReducerClass(ReducerStripe.class);
    
    job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(MapWritable.class);

	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}