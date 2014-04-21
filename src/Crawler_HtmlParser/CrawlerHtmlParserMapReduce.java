package Crawler_HtmlParser;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.htmlparser.util.ParserException;



public class CrawlerHtmlParserMapReduce {
	private static String level="1";
	
	private static String HtmlFilesPath="hdfs://ubuntu:9000/Crawler/HtmlFiles";
	private static String urlOutputPath="hdfs://ubuntu:9000/Crawler/urlOutput/"+level+".txt";
	public static class Map extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text link = new Text();

		public void map(LongWritable key, Text inputHtml,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
			//attention,the inputHtml is the html file
				try {
					ArrayList  linkArray=PageParser.getLinks(inputHtml.toString());
					if(linkArray==null){
						return;
					}
					if(linkArray.size()<=0){
						return;
					}
					for(int i=0;i<linkArray.size();i++){
						System.out.println(linkArray.get(i));
						output.collect(new Text(linkArray.get(i).toString()),one);
					}
				} catch (ParserException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	public static class Reduce extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable> {
		  public void reduce(Text url, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter){
			  //int status=DownloadPage.getStatus(url.toString());
			  //if (status >= 200 && status < 300) {
				  //new CrawlerThread(url.toString(),outPutPath);
			  //}
		  }
	}
	public static void main(String[] args)throws Exception{ 
		JobConf conf = new JobConf(CrawlerHtmlParserMapReduce.class);
		String inputPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/www.36kr.com_.html";
		String outputPath="hdfs://ubuntu:9000/Crawler/temp/temp3";
		conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
	    
		conf.setJobName("test");
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
//	    FileOutputFormat.setOutputPath(conf, new Path(outPut));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	    JobClient.runJob(conf);
		/*
		Path[] listedPaths=ReadHDFSFile.getFilePath(HtmlFilesPath);
		String temp;
		for(Path p:listedPaths){
			temp=ReadHDFSFile.getFileString(p.toString());
			WriteURLToHDFS.writeURLToHDFS(PageParser.getHtmlNode(temp), urlOutputPath);
	    }
	    */
	}
}
