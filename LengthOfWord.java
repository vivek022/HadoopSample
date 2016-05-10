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


public class LengthOfWord {
	
	public class LengthOfWordMap extends Mapper<Object, Text, IntWritable, IntWritable>{
		private IntWritable one = new IntWritable(1);
		private IntWritable wordLength = new IntWritable();

		public void map(Object key, Text value, Context context)
	            throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	            wordLength.set(tokenizer.nextToken().length());
	            context.write(wordLength, one);
	        }
	    }
	}
	
	public class LengthOfWordRed extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
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
	    		 System.err.println("Usage: LengthOfWordDriver <in> <out>");
	    		 System.exit(2);
	    		}
	    Job job = new Job(conf,"LengthOfWordDriver");

	    job.setJarByClass(LengthOfWordDriver.class);
	    job.setMapperClass(LengthOfWordMap.class);
	    job.setReducerClass(LengthOfWordRed.class);

	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // TODO: specify output types
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.waitForCompletion(true);
	}


}
