package npu.edu.hadoop1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReverseWord {
	
	public class ReverseMap extends Mapper<Object, Text, Text,NullWritable>{
		NullWritable nw = NullWritable.get();
	    private Text word = new Text();
	    public void map(Object arg0, Text value,
	    		Context context)
	            throws IOException {
	        String line = value.toString().concat("\n");
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	            word.set(new StringBuffer(tokenizer.nextToken()).reverse().toString());
	            
	            try {
					context.write(word,nw);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	          }

	    }

	}
	
	public class ReverseReduce extends
	Reducer<Text, NullWritable, Text, NullWritable> {
	    
	    public void reduce(Text arg1,NullWritable val,
	    		Context context)
	            throws IOException {
	        
	        try {
				context.write(arg1,val);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	    }

	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		 Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf,
		    		args).getRemainingArgs();
		    		if (otherArgs.length != 2) {
		    		 System.err.println("Usage: ReverseWord <in> <out>");
		    		 System.exit(2);
		    		}
		    Job job = new Job(conf,"ReverseWord");

		    job.setJarByClass(ReverseWord.class);
		    job.setMapperClass(ReverseMap.class);

		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(NullWritable.class);

		    // TODO: specify output types
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(NullWritable.class);

		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		   			try {
						job.waitForCompletion(true);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			

	  }
	
	
	
	

}
