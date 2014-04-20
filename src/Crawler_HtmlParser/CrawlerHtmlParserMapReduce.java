package Crawler_HtmlParser;

import org.apache.hadoop.fs.Path;


public class CrawlerHtmlParserMapReduce {
	private static String level="1";
	
	private static String HtmlFilesPath="hdfs://ubuntu:9000/Crawler/HtmlFiles";
	private static String urlOutputPath="hdfs://ubuntu:9000/Crawler/urlOutput/"+level+".txt";
	public static void main(String[] args)throws Exception{ 
		Path[] listedPaths=ReadHDFSFile.getFilePath(HtmlFilesPath);
		String temp;
		for(Path p:listedPaths){
			temp=ReadHDFSFile.getFileString(p.toString());
			WriteURLToHDFS.writeURLToHDFS(PageParser.getHtmlNode(temp), urlOutputPath);
	    }
	}
}
