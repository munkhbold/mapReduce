package part4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Framework {
  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Part4");
	    
	    job.setJarByClass(Framework.class);
	    job.setMapperClass(MapperClass.class);

	    job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(PairW.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}