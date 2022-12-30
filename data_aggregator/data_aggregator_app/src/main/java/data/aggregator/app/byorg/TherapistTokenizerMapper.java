package data.aggregator.app.byorg;

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
			// Input is coming in with this format "{period},{orgId},{therId}"
			String[] tokens = token.split(",");

			// Construct a new key
			String newKey = String.format("%s,%s", tokens[0], tokens[1]); 

			if(map.containsKey(newKey)) {
				int totalThers = map.get(newKey).intValue() + 1;

				map.put(newKey, totalThers);

				continue;
			}

			map.put(newKey, 1);
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

			String key = entry.getKey();

			int totalThers = entry.getValue().intValue();

			context.write(new Text(key), new IntWritable(totalThers));
		}

		map.clear();
	}
}
