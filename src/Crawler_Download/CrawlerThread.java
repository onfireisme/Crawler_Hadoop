package Crawler_Download;

import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;





   public class CrawlerThread implements Callable {
	    private String path;

        CrawlerThread( FileSystem fileSystem,String url,String HtmlFilePath,String HtmlInfoFilePath) {
        	path=DownloadPage.saveToHdfs(fileSystem, url.toString(),
					HtmlFilePath, HtmlInfoFilePath);
        }

        public Object call() throws Exception {
            return path;
        }
    }

/*
public class CrawlerThread implements Runnable {
	private Thread crawlerThread;
	//private static String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
	private String Url;
	private String outPutPath;
	private byte []PageBody;
	CrawlerThread(byte[] pageBody, String url,String tempOutPutPath){
		crawlerThread =new Thread(this);
		Url=url;
		outPutPath=tempOutPutPath;
		crawlerThread.start();
		pageBody=PageBody;
	}
	//this is the entry point for the new thread
	public void run(){
		PageBody=DownloadPage.downloadPage(Url,outPutPath);
	}
	
}
*/