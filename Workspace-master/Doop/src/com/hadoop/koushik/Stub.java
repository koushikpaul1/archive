package com.hadoop.koushik;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

class StubMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	private final Text k2 =  new Text();    	
	private IntWritable v2  =  new IntWritable(1);
	
    //@Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

    	
    	//Mapper logic
    		context.write(k2, v2);
    	}
    	
    	
    	
    	
    
}

class StubReducer extends Reducer<Text,  IntWritable, Text, IntWritable> {
	
		private Text k3=new Text();
		private IntWritable sum1=new IntWritable();
	

	
    //@Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
    	
    	//Reduce logic
    	context.write(k3, sum1);    	
    }
    
}

public class Stub {
    
	

	

    public static void main(String[] args) throws Exception { 
    	
    	Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: Stub <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(Stub.class);
		job.setMapperClass(StubMapper.class);
		job.setCombinerClass(StubReducer.class);
		job.setReducerClass(StubReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}



