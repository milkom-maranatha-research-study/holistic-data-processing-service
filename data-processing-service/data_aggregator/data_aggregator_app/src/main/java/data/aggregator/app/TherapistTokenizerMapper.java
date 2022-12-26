package data.aggregator.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * A mapper class that implements in-mapper combiner algorithm.
 * 
 * All incoming `text` will be calculated before we sent it to the Reducer class.
 */
public class TherapistTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static Map<String, Integer> map = new HashMap<String, Integer>();

	/**
	 * Calculate every unique token and put it on the class's map object.
	 * If the token doesn't exists, initialize it with 1. Otherwise, sum it with 1.
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
 
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();

			// Extracts tokens by removing comma delimiter
			// Input is in this format "{period},{orgId},{totalThersInOrg},{therId}"
			String[] tokens = token.split(",");

			// Construct a new key
			String keyValue = String.format("%s,%s,%s", tokens[0], tokens[1], tokens[2]); 

			if(map.containsKey(keyValue)) {
				int totalActiveThers = map.get(keyValue).intValue() + 1;

				map.put(keyValue, totalActiveThers);

				continue;
			}

			map.put(keyValue, 1);
		}
	}

	/**
	 * We override the `cleanup` method to write every map object into HDFS
	 * through the mapper's context.
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();

		while(iterator.hasNext()) {
			Map.Entry<String, Integer> entry = iterator.next();

			String keyValue = entry.getKey();

			int totalActiveThers = entry.getValue().intValue();

			context.write(new Text(keyValue), new IntWritable(totalActiveThers));
		}

		map.clear();
	}
}
