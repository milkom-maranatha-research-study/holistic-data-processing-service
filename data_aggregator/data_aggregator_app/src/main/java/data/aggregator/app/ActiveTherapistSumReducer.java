package data.aggregator.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** A Reducer class that receives inputs from the Mapper class. */
public class ActiveTherapistSumReducer extends Reducer<Text, IntWritable, Text, Text> {
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
		
		// New keys are defined by "{period},{orgId},{totalThersInOrg}"
		String strTotalTherFromKey = keys[2];

		finalKey.set(String.format("%s,%s", keys[0], keys[1]));

		int totalThersInKey = Integer.parseInt(strTotalTherFromKey);
		int totalInactiveThers = totalThersInKey - sumActiveThers;

		result.set(String.format("%d,%d\t%s", sumActiveThers, totalInactiveThers, strTotalTherFromKey));

		context.write(finalKey, result);
	}
}
