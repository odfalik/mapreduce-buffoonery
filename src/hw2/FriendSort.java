package hw2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendSort {
	
	public static class Map extends Mapper<LongWritable, Text, IdAgeWritableComparable, Text> {
		
		private HashMap<String, NameAgeWritable> table = new HashMap<String, NameAgeWritable>();	// (uid, (fname, age))
		
		// Initialize HashMap
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			for (URI path : cacheFiles) { 
				int last = path.toString().lastIndexOf("/");
				String filename = path.toString().substring(last+1);
				BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
				while (reader.ready()) {
					String line = reader.readLine();
					String[] attributes = line.split(",");
					table.put(attributes[0], new NameAgeWritable(attributes[1], calcAge(attributes[9])));
				}
				reader.close();
			}
		}
		
		private static int calcAge(String bday) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/y");
			LocalDate bdate = LocalDate.parse(bday, formatter);
			LocalDate today = LocalDate.now();
			return Period.between(bdate, today).getYears();
		}
		
		// Emit (uid, (fname, age_of_friend)) kv-pairs
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			
			if (line.length > 1) {
				for (String friend : line[1].split(",")) {
					if (table.containsKey(friend)) {
						context.write(
								new IdAgeWritableComparable(line[0], table.get(friend).age.get()),
								table.get(friend).fname
						);
					}
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<IdAgeWritableComparable, Text, Text, Text> {

		public void reduce(IdAgeWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> namesIterator = values.iterator();
			
			while (namesIterator.hasNext()) {
				Text name = namesIterator.next();
				
				context.write(key.id, new Text(name + "\t" + key.age.get()));
			}
			
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "FriendSort");
		job.setJarByClass(AvgFriendAge.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.addCacheFile(new URI(args[1]));
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(IdAgeWritableComparable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
