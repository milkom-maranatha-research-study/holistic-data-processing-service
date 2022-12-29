package data.aggregator.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** A Reducer class that receives inputs from the Mapper class. */
public class AllActiveTherapistSumReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Text result = new Text();
	private Text finalKey = new Text();

	/** Aggregates every value belonging to the key and write it to the HDFS Context. */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
		int sumActiveThers = 0;

		for (IntWritable val : values) {
			sumActiveThers += val.get();
		}
		
		// Get available keys
		String[] keys = key.toString().split(",");

		// New keys are defined by "{allTimePeriod},{allTimeThers}"
		finalKey.set(keys[0]);

		String strAllTimeThersFromKey = keys[1];
		
		int totalAllTimeThersInKey = Integer.parseInt(strAllTimeThersFromKey);
		int totalAllTimeInactiveThers = totalAllTimeThersInKey - sumActiveThers;

		result.set(String.format("%d,%d\t%s", sumActiveThers, totalAllTimeInactiveThers, strAllTimeThersFromKey));

		context.write(finalKey, result);
	}
}
