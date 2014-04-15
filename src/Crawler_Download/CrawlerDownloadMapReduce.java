package Crawler_Download;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


public class CrawlerDownloadMapReduce {
	/* in map part,as the stringTokenizer set as the space and changline as defualt to 
	* divide the string .At this part,we divide the string by the changeline"\n"
	* so we don't need to change the code here
	* and the ouput is ok for us
	*/
	private static String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
	private static String coreXMLPath;
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
          OutputCollector<Text, IntWritable> output, Reporter reporter)
          throws IOException {
			  new CrawlerThread(url.toString());
		  }
	}
	public void downloadPage(String url)  throws IOException{
		Configuration conf = new Configuration();
	      conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	      conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	      String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/"+DownloadPage.getFileNameByUrl(url.toString(),"html");
	      FileSystem fileSystem = FileSystem.get(conf);
	      if (fileSystem.exists(new Path(hdfsPath))) {
	          System.out.println("File " + hdfsPath + " already exists");
	          return;
	      }
	      byte[] b=new byte[1024];
	      FSDataOutputStream out = fileSystem.create(new Path(hdfsPath));
	      out.write(b);
	      out.close();
		  fileSystem.close();
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
		String filenamePath="hdfs://ubuntu:9000/Crawler/inputUrl/url.txt";
		String outPut="hdfs://ubuntu:9000/Crawler/HtmlFiles/mytest";
		JobConf conf = new JobConf(CrawlerDownloadMapReduce.class);
		
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
