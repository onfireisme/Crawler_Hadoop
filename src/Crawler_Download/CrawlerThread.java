package Crawler_Download;


public class CrawlerThread implements Runnable {
	private Thread crawlerThread;
	private static String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
	private String Url;
	CrawlerThread(String url){
		crawlerThread =new Thread(this);
		Url=url;
		crawlerThread.start();
	}
	//this is the entry point for the new thread
	public void run(){
		DownloadPage downloadPage=new DownloadPage();
		try {
			downloadPage.downloadPage(Url,hdfsPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("some thing wrong with the downloadPage function");
			e.printStackTrace();
		}
	}
	
}