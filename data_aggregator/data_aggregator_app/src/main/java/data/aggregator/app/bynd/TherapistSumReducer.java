package data.aggregator.app.bynd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** A Reducer class that receives inputs from the Mapper class. */
public class TherapistSumReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Text result = new Text();

	/** Aggregates every value belonging to the key and write it to the HDFS Context. */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
		int sumThers = 0;

		for (IntWritable val : values) {
			sumThers += val.get();
		}

		result.set(String.valueOf(sumThers));

		context.write(key, result);
	}
}
