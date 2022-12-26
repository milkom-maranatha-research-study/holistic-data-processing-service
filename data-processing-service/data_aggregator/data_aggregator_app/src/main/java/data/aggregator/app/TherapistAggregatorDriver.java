package data.aggregator.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Data Processing Service (DPS) main class,
 * an entry point for executing Hadoop Map-Reduce.
 */
public class TherapistAggregatorDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = new Configuration();
		
		JobConf jobConf = new JobConf(config, TherapistAggregatorDriver.class);

		Path inputPath = new Path(args[0]);
    	FileInputFormat.setInputPaths(jobConf, inputPath);

    	Path outputPath = new Path(args[1]);
    	FileOutputFormat.setOutputPath(jobConf, outputPath);
		outputPath.getFileSystem(config).delete(outputPath, true);

		Job job = getJob(jobConf);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	private Job getJob(JobConf jobConfig) throws IOException {
		Job job = Job.getInstance(jobConfig, "MR Job - Aggregate Active/Inactive Therapists");

		job.setMapperClass(TherapistTokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(TherapistSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TherapistAggregatorDriver(), args);

		// TODO: Get output, send it to the Backend App?
        System.exit(exitCode);
    }
}
