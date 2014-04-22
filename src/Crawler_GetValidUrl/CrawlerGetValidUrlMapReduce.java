package Crawler_GetValidUrl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import onfire.configure.configure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Crawler_MergeUrl.CrawlerMergeUrlMapReduce;
import Crawler_MergeUrl.CrawlerMergeUrlMapReduce.CrawlerHtmlParserReduce;
import Crawler_MergeUrl.CrawlerMergeUrlMapReduce.MapClass;

public class CrawlerGetValidUrlMapReduce extends Configured implements Tool  {
	//private static String urlFilePath=configure.URLFILESPATH +"2"+ configure.URLNAME;
	//private static String urlFilePath="hdfs://ubuntu:9000/Crawler/UrlFiles/2/merge/part-r-00000";
	//private static String urlFilePath2="hdfs://ubuntu:9000/Crawler/UrlFiles/1/url.txt";
	private static ArrayList urlFilePath;
	private static String validUrlFileFolderPath;

	//private static String temp=configure.URLFILESPATH+"2"+"/valid";
	public static class MapClass extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text url = new Text();
		private final static IntWritable one = new IntWritable(1);
		private Text temp = new Text();
		private ArrayList linkList = null;

		// Map Method
		public void map(LongWritable key, Text filePathInfo, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(
					filePathInfo.toString());
			while (tokenizer.hasMoreTokens()) {
				url.set(tokenizer.nextToken());
				context.write(url, one);
			}
		}

		@Override
		protected void cleanup(Context context) {
			// at this place,we can delete the 
			// this function is called when the mapreduce is finished
		}
	}

	public static class CrawlerHtmlParserReduce extends
			Reducer<Text, IntWritable, Text, NullWritable> {
		private final static IntWritable one = new IntWritable(1);

		// Reduce Method
		public void reduce(Text url, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.out.println(url);
			int sum=0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum==1){
				context.write(url, NullWritable.get());
			}
		}
		/*
		 * @Override protected void cleanup(Context context) throws IOException,
		 * InterruptedException { //maybe I can put all the url to one at this
		 * place,or I can write a function to get all the }
		 */
	}
	public void initPath() throws IOException {
		urlFilePath=configure.getLatestMergedUrlFilePath();
		validUrlFileFolderPath=configure.getLatestValidUrlFolderPath();
		/*
		 * urlFilePath=configure.URLFILESPATH + Integer.toString(intLevel) +
		 * "/";
		 */
		System.out.println(urlFilePath);
	}

	public int run(String[] arg0) throws Exception {
		//setLevel("1");
		initPath();
		Job job = new Job();
		job.setJarByClass(CrawlerGetValidUrlMapReduce.class);
		for(int i=0;i<urlFilePath.size();i++){
			FileInputFormat.addInputPath(job, new Path(urlFilePath.get(i).toString()));
		}
		FileOutputFormat.setOutputPath(job, new Path(validUrlFileFolderPath));

		job.setMapperClass(MapClass.class);
		// job.setCombinerClass(CrawlerHtmlParserReduce.class);
		job.setReducerClass(CrawlerHtmlParserReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// job.waitForCompletion(true);
		// return 0;
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new CrawlerGetValidUrlMapReduce(), args);
		// System.exit(res);
	}
}
