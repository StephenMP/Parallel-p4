import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Movie Data Miner Project
 */
public class MovieDataMiner {
	public static class MovieRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text year = new Text();
		private final static Text rating = new Text();

		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
			/* Get line from val to tokenize */
			String line = val.toString();

			/* Create our tokenizer (strip punctuation) */
			StringTokenizer itr = new StringTokenizer(line.toLowerCase(), " , .;:'\"&!?-_\n\t[]<>\\`~|=^()@#$%^*/+-");

			/* For each data set, map the year and rating w/ year as key */
			String token;
			while (itr.hasMoreTokens()) {
				token = itr.nextToken();
				
				if(token.equals("{"))
				{
					year.set(itr.nextToken());
					rating.set(itr.nextToken());
					context.write(year, rating);
				}
			}
		}
	}

	public static class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		/**
		 * key - Will contain date post mapping
		 * values - Will contain rating post mapping
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<Integer, MovieData> hashTable = new HashMap<Integer, MovieData>();
			Iterator<Text> itr = values.iterator();
			StringBuilder toReturn = new StringBuilder();

			/* Iterate through mapped ratings and insert them into the HashMap */
			while (itr.hasNext()) {
				Text next = itr.next();
				
			}
			
			//TODO: Iterate map keys and sort into decades

			/* Write our result to the context */
			context.write(key, new Text(toReturn.toString()));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		if (args.length < 2 || args.length > 3) {
			System.out.println("Usage: MovieDataMinder <input path> <output path>");
			System.exit(1);
		}
		
		Job job = new Job(conf, "MovieDataMiner");
		job.setJarByClass(MovieDataMiner.class);
		job.setMapperClass(MovieRatingMapper.class);
		job.setReducerClass(MovieRatingReducer.class);
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
