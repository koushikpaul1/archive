package com.hadoop.koushik.mapred;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/*import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;*/



public class WordCount {
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("WordCount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(WordCountReducer.class);		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		System.out.println("Done!");
	}
}


/* class DescendingKeyComparator extends WritableComparator {
    protected DescendingKeyComparator() {
        super(Text.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;          
        return key2.compareTo(key1);
    }
}*/