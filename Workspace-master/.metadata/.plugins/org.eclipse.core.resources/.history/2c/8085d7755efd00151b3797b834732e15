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
import java.util.Arrays;
import java.util.StringTokenizer;

class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text k2 = new Text();

	//@Override
	protected void map(LongWritable k1, Text v1, Context context)
			throws IOException, InterruptedException {
		String word = v1.toString();
		char[] carr = word.toCharArray();
		Arrays.sort(carr);
		k2.set(new String(carr));
		context.write(k2, v1);
	}
}

class AnagramReducer extends Reducer<Text, Text, Text, Text> { 

	private Text valuev3 = new Text();

	//@Override
	protected void reduce(Text k3, Iterable<Text> v3, Context context)
			throws IOException, InterruptedException {
		String word = "";
		for (Text sample : v3) {
			word += sample.toString() + " ";
		}
		String[] sarr = word.split(" ");
		if (sarr.length > 1) {
			valuev3.set(word);
			context.write(k3, valuev3);
		}
	}

}

public class Anagram {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: Anagram <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Anagram  ");
		job.setJarByClass(Anagram.class);
		job.setMapperClass(AnagramMapper.class);
		job.setCombinerClass(AnagramReducer.class);
		job.setReducerClass(AnagramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
