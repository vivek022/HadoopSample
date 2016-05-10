package npu.edu.hadoop1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TotalWordCount {
	
	public class TotalWordMap extends Mapper<Object, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		private Text sum = new Text();
		public void map(Object key, Text value, Context context
	        ) throws IOException, InterruptedException {
		
	    StringTokenizer itr = new StringTokenizer(value.toString());
	    while (itr.hasMoreTokens()) {
	    	 sum.set(itr.nextToken());
	    	 context.write(new Text("Sum"), one);
	    	 }
	    
		}
	  
	}
	
	public class TotalWordReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		    throws IOException, InterruptedException {
		int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
			}
		}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf,
	    		args).getRemainingArgs();
	    		if (otherArgs.length != 2) {
	    		 System.err.println("Usage: TotalWordCount <in> <out>");
	    		 System.exit(2);
	    		}
	    Job job = new Job(conf,"TotalWordCount");

	    job.setJarByClass(TotalWordCount.class);
	    job.setMapperClass(TotalWordMap.class);
	    job.setReducerClass(TotalWordReduce.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // TODO: specify output types
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.waitForCompletion(true);
	}
}
