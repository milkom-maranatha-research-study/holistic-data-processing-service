package holistic.dps;

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
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

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

			if(map.containsKey(token)) {
				int total = map.get(token).intValue() + 1;

				map.put(token, total);

				continue;
			}

			map.put(token, 1);
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
			int total = entry.getValue().intValue();
			
			context.write(new Text(keyValue), new IntWritable(total));
		}

		map.clear();
	}
}
