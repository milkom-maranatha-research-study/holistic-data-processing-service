package data.aggregator.app.byorg;

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

		boolean isAggregateAll = "all".equals(args[2]);
		Job job = getJob(jobConf, isAggregateAll);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	private Job getJob(JobConf jobConfig, boolean isAggregateAll) throws IOException {
		Job job = Job.getInstance(jobConfig, "MR Job - Aggregate Number of Therapists per Org");

		job.setMapperClass(isAggregateAll ? AllTherapistTokenizerMapper.class : TherapistTokenizerMapper.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(isAggregateAll ? AllTherapistSumReducer.class : TherapistSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TherapistAggregatorDriver(), args);
        System.exit(exitCode);
    }
}
