package data.aggregator.app;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** A Reducer class that receives inputs from the Mapper class. */
public class TherapistSumReducer extends Reducer<Text, IntWritable, Text, ObjectWritable> {
	private ObjectWritable result = new ObjectWritable();

	/** Aggregates every value belonging to the key and write it to the HDFS Context. */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
		int sumActiveThers = 0;

		for (IntWritable val : values) {
			sumActiveThers += val.get();
		}
		
		// Get available keys
		String[] keys = key.toString().split(",");
		
		// Keys are constructed from "{period},{orgId},{totalThersInOrg}"
		String strTotalTherFromKey = keys[2];

		int totalThersInKey = Integer.parseInt(strTotalTherFromKey);
		int totalInactiveThers = sumActiveThers - totalThersInKey;

		result.set(String.format("%d,%d", sumActiveThers, totalInactiveThers));

		context.write(key, result);
	}
}
