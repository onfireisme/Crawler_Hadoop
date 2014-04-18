package Crawler_Download;


public class CrawlerThread implements Runnable {
	private Thread crawlerThread;
	//private static String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
	private String Url;
	private String outPutPath;
	CrawlerThread(String url,String tempOutPutPath){
		crawlerThread =new Thread(this);
		Url=url;
		outPutPath=tempOutPutPath;
		crawlerThread.start();
	}
	//this is the entry point for the new thread
	public void run(){
		DownloadPage.saveToHdfs(Url,outPutPath);
	}
	
}