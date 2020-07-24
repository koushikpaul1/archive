package com.hadoop.koushik;

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

public class HashTag {

	public static class HashTagMapper extends Mapper<Object, Text, Text, Text> {
		private static final Text hash = new Text("#");
		private Text word = new Text();
		

		public void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				this.word.set(itr.nextToken());
				if (word.toString().startsWith("#"))
					context.write(this.word, hash);
				
			}
		}
	}

	public static class HashTagReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				sum += 1;
			}
			this.result.set(sum);
			context.write(key, this.result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: HashTag <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "HashTag");
		job.setJarByClass(HashTag.class);
		job.setMapperClass(HashTagMapper.class);
		job.setCombinerClass(HashTagReducer.class);
		job.setReducerClass(HashTagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}