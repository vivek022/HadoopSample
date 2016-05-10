package npu.edu.hadoop1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class VowelCons {

public class VowelConsMapper extends Mapper {
	
	public void map(LongWritable mapKey,Text mapValue,Context context) throws IOException, InterruptedException{

	    String line = mapValue.toString();
	    String[] letters = line.split("");

	    for(String letter : letters){
	        System.out.println(letter);
	        if(letter!=" "){
	            if(isVowel(letter))
	                context.write(new Text("Vowel"), new IntWritable(1));
	            else
	                context.write(new Text("Consonant"), new IntWritable(1));
	        }
	    }
	}

	private boolean isVowel(String letter) {
	    // TODO Auto-generated method stub
	    if(letter.equalsIgnoreCase("a")||letter.equalsIgnoreCase("e")||letter.equalsIgnoreCase("i")||letter.equalsIgnoreCase("o")||letter.equalsIgnoreCase("u"))
	        return true;
	    else
	        return false;
	}
}
	public class VowelConsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text letterType,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable value:values){
            sum += value.get();
        }
        System.out.println(letterType+"     "+sum);
        context.write(letterType, new IntWritable(sum));
    }

	}
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,
        		args).getRemainingArgs();
        		if (otherArgs.length != 2) {
        		 System.err.println("Usage: VowelConsDriver <in> <out>");
        		 System.exit(2);
        		}
        Job job = new Job(conf,"VowelConsDriver");

        job.setJarByClass(VowelCons.class);
        job.setMapperClass(VowelConsMapper.class);
        job.setCombinerClass(VowelConsReducer.class);
        job.setReducerClass(VowelConsReducer.class);

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

	

