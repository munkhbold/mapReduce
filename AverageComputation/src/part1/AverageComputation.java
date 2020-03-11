package part1;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageComputation {
//	private static Logger logger = Logger.getLogger(TokenizerMapper.class);
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, PairWritable>{
		private HashMap<String, PairWritable> buf;
		
		PairWritable pair = new PairWritable();

		@Override
		public void setup(Context context) {
			 buf = new HashMap<>();
		}

		public boolean isCorrect(String k, String v) {
			String digitOnly = "\\d+";
		    String ipOnly = "\\d+\\.\\d+.\\d+\\.\\d+";
			return k.matches(ipOnly) && v.matches(digitOnly);
		}
		
		@Override
		public void map(Object key, Text value, Context context){
		    String[] record = value.toString().split(" ");
		
		    String ipAddress = record[0];
		    String v = record[record.length-1];
		    
		    if(!isCorrect(ipAddress, v))
		    	return;
		
		    if(buf.containsKey(ipAddress)) {
		    	pair = buf.get(ipAddress);
		    	pair.setKey(pair.getKey() + Integer.parseInt(v));
		    	pair.setValue(pair.getValue() + 1);
		    }else {
		    	pair.setKey(Integer.parseInt(v));
		    	pair.setValue(1);
		    }
		
		    buf.put(ipAddress, pair);
		}
		
		private Text ip = new Text();
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(String k: buf.keySet()) {
				ip.set(k);
				context.write(ip, buf.get(k));
			}
		}
  }

  // Reducer class
  public static class IntSumReducer extends Reducer<Text, PairWritable, Text, FloatWritable> {
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
  
  public static class Combiner extends Reducer<Text, PairWritable, Text, PairWritable> {

	    @Override
	    protected void reduce(Text key, Iterable<PairWritable> values, Context context)
	            throws IOException, InterruptedException {

	        super.reduce(key, values, context);

	    }
	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average computation");
    
    job.setJarByClass(AverageComputation.class);
    job.setMapperClass(TokenizerMapper.class);

    job.setCombinerClass(Combiner.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);

	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}