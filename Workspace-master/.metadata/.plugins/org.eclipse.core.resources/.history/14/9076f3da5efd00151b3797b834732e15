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

class UniqueWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final Text k2 = new Text();
	private IntWritable v2 = new IntWritable(1);

	//@Override
	protected void map(LongWritable k1, Text v1, Context context)
			throws IOException, InterruptedException {

		StringTokenizer st = new StringTokenizer(v1.toString());
		while (st.hasMoreTokens()) {
			k2.set(st.nextToken());
			context.write(k2, v2);
		}

	}
}

class UniqueWordReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text v33 = new Text();
	

	//@Override
	protected void reduce(Text k3, Iterable<IntWritable> v3, Context context)
			throws IOException, InterruptedException {
		v33.set("");
		context.write(k3, v33);
	}

}

public class UniqueWord {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: UniqueWord <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(UniqueWord.class);
		job.setMapperClass(UniqueWordMapper.class);
		job.setCombinerClass(UniqueWordReducer.class);
		job.setReducerClass(UniqueWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
