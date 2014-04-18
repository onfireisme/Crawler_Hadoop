package Crawler_Download;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class CrawlerDownloadMapReduce {
	/* in map part,as the stringTokenizer set as the space and changline as defualt to 
	* divide the string .At this part,we divide the string by the changeline"\n"
	* so we don't need to change the code here
	* and the ouput is ok for us
	*/
	private static String outPutPath;
	//private static String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
	public static class Map extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text reduceInputUrl = new Text();

		public void map(LongWritable key, Text mapInputUrl,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
			String url = mapInputUrl.toString();
			StringTokenizer tokenizer = new StringTokenizer(url);
			while (tokenizer.hasMoreTokens()) {
				reduceInputUrl.set(tokenizer.nextToken());
				if(reduceInputUrl.toString() != null && reduceInputUrl.toString().length() > 0){
					//if the url is null ,then we do nothing
					output.collect(reduceInputUrl, one);
				}
			}
		}
	}
	/*
	 * the difficult part is the reducer part,we need to output multi html files
	 * and we need to crawler the file by multi-threads
	 */
	public static class Reduce extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
		  public void reduce(Text url, Iterator<IntWritable> values,
          OutputCollector<Text, IntWritable> output, Reporter reporter){
			  //int status=DownloadPage.getStatus(url.toString());
			  //if (status >= 200 && status < 300) {
				  new CrawlerThread(url.toString(),outPutPath);
			  //}
		  }
	}
	public static void main(String[] args)throws Exception{ 
		/*
		Configuration conf = new Configuration();
        conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
        String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/test.txt";
        FileSystem fileSystem = FileSystem.get(conf);
        byte[] b=new byte[1024];
        FSDataOutputStream out = fileSystem.create(new Path(hdfsPath));
        out.write(b);
		out.close();
		fileSystem.close();
		*/
		
		String filenamePath="hdfs://ubuntu:9000/Crawler/url.txt";
		String outPut="hdfs://ubuntu:9000/Crawler/HtmlFiles/mytest3";
		//String filenamePath=args[0];
		//String outPut=args[1];
		//outPutPath=args[2];
		outPutPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
		JobConf conf = new JobConf(CrawlerDownloadMapReduce.class);
		conf.setBoolean("mapreduce.job.user.classpath.first", true);

		//conf.setUserClassesTakesPrecedence(true);
		
		conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
	    
		conf.setJobName("test");
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(filenamePath));
	    FileOutputFormat.setOutputPath(conf, new Path(outPut));
	    JobClient.runJob(conf);
	
	}
}
