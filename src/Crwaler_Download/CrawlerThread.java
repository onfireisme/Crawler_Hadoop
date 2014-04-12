package Crwaler_Download;


public class CrawlerThread implements Runnable {
	private Thread crawlerThread;
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
			downloadPage.downloadPage(Url);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("some thing wrong with the downloadPage function");
			e.printStackTrace();
		}
	}
	
}