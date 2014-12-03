import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Movie Data Miner Project
 */
public class MovieDataMiner {
	public static class InvertedIndexMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private final static Text word = new Text();
		private final static Text location = new Text();

		@Override
		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {

			/* Split up the files */
			FileSplit fileSplit = (FileSplit) context.getInputSplit();

			/* Get our filename */
			String fileName = fileSplit.getPath().getName();

			/* Set location to the filename */
			location.set(fileName);

			/* Get line from val to tokenize */
			String line = val.toString();

			/* Create our tokenizer (strip non-words) */
			StringTokenizer itr = new StringTokenizer(line.toLowerCase(),
					" , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-");

			/*
			 * For each word, set it and write it along with where it was found
			 * (word as our key)
			 */
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, location);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, Text, Text, Text> {

		/* Generic sorter for Map collections */
		private <K, V extends Comparable<? super V>> Map<K, V> sortMap(Map<K, V> map) {
			/* Copy in our map to a List so we can sort properly */
			List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
			
			/* Sort it using custom comparator */
			Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
				@Override
				public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
					/* Order from greatest to smallest */
					int returnVal = -1 * (o1.getValue()).compareTo(o2.getValue());

					/* If they equal, we want to sort on the filenames */
					if (returnVal == 0)
						return (o1.getKey().toString().compareTo(o2.getKey().toString()));
					
					/* Otherwise, keep the sort by value */
					else
						return returnVal;
				}
			});

			/* Create new LinkedHashMap to preserve order */
			Map<K, V> result = new LinkedHashMap<>();
			
			/* Copy in values from our sorted map */
			for (Map.Entry<K, V> entry : list)
				result.put(entry.getKey(), entry.getValue());
			
			/* Send the copy back (a copy eliminated ConcurrenceException) */
			return result;
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> map = new HashMap<String, Integer>();
			Iterator<Text> itr = values.iterator();
			StringBuilder toReturn = new StringBuilder();
			boolean first = true;

			/* Iterate over filenames related to our key */
			while (itr.hasNext()) {
				/* We will use the filename as a key in our HashMap */
				Text next = itr.next();
				String mapKey = next.toString();

				/* If it doesn't exist, put in in the HashMap */
				if (!map.containsKey(mapKey))
					map.put(mapKey, 1);
				/* Otherwise, increment our count */
				else
					map.put(mapKey, map.get(next.toString()) + 1);
			}

			/* Get a sorted version of our map */
			map = sortMap(map);

			/* Iterate our keys and append our results */
			for (String mapKey : map.keySet()) {
				if (!first)
					toReturn.append(", ");
				
				first = false;
				toReturn.append(map.get(mapKey) + " " + mapKey);
			}

			/* Write our result to the context */
			context.write(key, new Text(toReturn.toString()));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		if (args.length < 2 || args.length > 3) {
			System.out
					.println("Usage: InvertedIndex <input path> <output path>");
			System.exit(1);
		}
		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(MovieDataMiner.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (args.length == 2) {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		}
		else {
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
		}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
