package reverse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Text word =  new Text();    	
	private IntWritable count  =  new IntWritable(1);	
    //@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {    	
    	StringTokenizer st=new StringTokenizer(value.toString());
    	while (st.hasMoreTokens()){ 
    		word.set(st.nextToken());
    		context.write(word, count);
    	}    	
    }
}

class WordCountReducer extends Reducer<Text,  IntWritable, Text, IntWritable> {		
		private IntWritable sum1=new IntWritable();	
    //@Override
    protected void reduce(Text word, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException {
    	
    	int sum=0;    	
    	for(IntWritable value : count){    		
		   sum+=value.get();        	
    	}
    	sum1.set(sum);
    	context.write(word, sum1);    	
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
		job.setSortComparatorClass(DescendingKeyComparator.class);
		//job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }    
}

class DescendingKeyComparator extends WritableComparator {
    protected DescendingKeyComparator() {
        super(Text.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;          
        return -1 * key1.compareTo(key2);
    }
}




