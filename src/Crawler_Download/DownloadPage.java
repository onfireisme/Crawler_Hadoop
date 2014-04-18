package Crawler_Download;


import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.http.client.methods.HttpGet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DownloadPage {
	public static String getFileNameByUrl(String url,String contentType)
	{
		url=url.substring(7);//remove http://
		if(contentType.indexOf("html")!=-1)//text/html
		{
			url= url.replaceAll("[\\?/:*|<>\"]", "_")+".html";
			return url;
		}
		else//如application/pdf
		{
			return url.replaceAll("[\\?/:*|<>\"]", "_")+"."+ 
		contentType.substring(contentType.lastIndexOf("/")+1);
		}	
	}
	public static void saveToHdfs(String url,String hdfsPath){
		byte[] responseBody=downloadPage(url,hdfsPath);
		String fileHdfsPath=null;
		if(responseBody!=null){
			fileHdfsPath=hdfsPath+getFileNameByUrl(url,"html");
			Configuration conf = new Configuration();
		    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
		    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
		    // String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
		    FileSystem fileSystem;
		    try {
				fileSystem = FileSystem.get(conf);
					 FSDataOutputStream out;
					 System.out.println("yes");
					 if (fileSystem.exists(new Path(fileHdfsPath))) {
					          System.out.println("File " + hdfsPath + " already exists");
					          return;
					 }
					 out= fileSystem.create(new Path(fileHdfsPath));
					 out.write(responseBody);
					 out.close();
					 fileSystem.close();  
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		else{
			System.out.println("the file is null");
		}
	}
	public static int getStatus(String url){
		HttpClient httpClient = new HttpClient();
		httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
		GetMethod getMethod=new GetMethod(url);	 
		getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT,5000);
		  //设置请求重试处理，用的是默认的重试处理：请求三次
		getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
		          new DefaultHttpMethodRetryHandler());
		HttpGet httpget = new HttpGet(url);
		int status=0;
		try {
			status = httpClient.executeMethod(getMethod);
		} catch (HttpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return status;
        
	}
	//this time ,we just return the page data
	public static byte [] downloadPage(String url,String hdfsPath){
		HttpClient httpClient = new HttpClient();
		httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(5000);
		GetMethod getMethod=new GetMethod(url);	 
		getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT,5000);
		  //设置请求重试处理，用的是默认的重试处理：请求三次
		getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
		          new DefaultHttpMethodRetryHandler());
		byte[] responseBody;
		try {
			int status = httpClient.executeMethod(getMethod);
			if (status >= 200 && status < 300) {
				responseBody = getMethod.getResponseBody();
				return responseBody;
			}
		} catch (HttpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //configuration of hadoop file system
		responseBody=null;
		return responseBody;
            
	}

}