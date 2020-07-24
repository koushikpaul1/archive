package setGroupingComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Secondary sort job.
 * @author Jee Vang
 *
 */
public class SsJob extends Configured implements Tool {

	/**
	 * Main method. You should specify -Dmapred.input.dir and -Dmapred.output.dir.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SsJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "secondary sort");
		
		job.setJarByClass(SsJob.class);
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		job.setMapOutputKeyClass(StockKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SsMapper.class);
		job.setReducerClass(SsReducer.class);
		
		job.waitForCompletion(true);
		
		return 0;
	}

}