package data.aggregator.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** A Reducer class that receives inputs from the Mapper class. */
public class TherapistPerOrgSumReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Text result = new Text();
	private Text finalKey = new Text();

	/** Aggregates every value belonging to the key and write it to the HDFS Context. */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
		int sumThers = 0;

		for (IntWritable val : values) {
			sumThers += val.get();
		}

		// Get available keys
		String[] keys = key.toString().split(",");
		
		// New keys are defined by "{period},{orgId}"
		String newKey = keys[0];
		String orgId = keys[1];

		finalKey.set(newKey);

		result.set(String.format("%s\t%d", orgId, sumThers));

		context.write(finalKey, result);
	}
}
