package partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final Text word =  new Text();    	
	private IntWritable one  =  new IntWritable(1);	
    //@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    	
    	StringTokenizer st=new StringTokenizer(value.toString());
    	while (st.hasMoreTokens()){ 
    		word.set(st.nextToken());
    		context.write(word, one);
    	}    	
    }
}



class WordCountReducer extends Reducer<Text,  IntWritable, Text, IntWritable> {	
		
		private IntWritable count=new IntWritable();	
    //@Override
    protected void reduce(Text word, Iterable<IntWritable> onelist, Context context) throws IOException, InterruptedException {    	
    	int sum=0;    	
    	for(IntWritable one : onelist){    		
		   sum+=one.get();        	
    	}
    	count.set(sum);
    	context.write(word, count);    	
    }    
}



public class WordCount {
    public static void main(String[] args) throws Exception { 
    	
    	Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//Number of reducers
		job.setNumReduceTasks(2);
		//specifying the custom partitioner class.
		job.setPartitionerClass(WordCountPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}


class WordCountPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		if(numPartitions == 2){
			String partitionKey = key.toString();
			if(partitionKey.charAt(0) < 'a' )
				return 0;
			else 
				return 1;
		}else if(numPartitions == 1)
			return 0;
		else{
			System.err.println("WordCountParitioner can only handle either 1 or 2 paritions");
			return 0;
		}
	}
}



