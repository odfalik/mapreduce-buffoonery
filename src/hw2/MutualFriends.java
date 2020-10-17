package hw2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text pair = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				int uid = Integer.parseInt(line[0]);
				int fid;
				List<String> friends = Arrays.asList(line[1].split(","));
				for (String friendIdString : friends) {
					fid = Integer.parseInt(friendIdString);
					pair.set((uid < fid) ? uid+","+fid : fid+","+uid);
					context.write(pair, new Text(line[1]));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			StringBuilder sb = new StringBuilder();
			for (Text friends : values) {
				List<String> temp = Arrays.asList(friends.toString().split(","));
				for (String friend : temp) {
					if (map.containsKey(friend))
						sb.append(friend + ',');
					else
						map.put(friend, 1);
				}
			}
			if (sb.lastIndexOf(",") > -1) {
				sb.deleteCharAt(sb.lastIndexOf(","));
			}

			result.set(new Text(sb.toString()));
			context.write(key, result);
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
