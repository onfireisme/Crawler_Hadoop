package Crawler_Download;


import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
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
	}
	//this time ,we just return the page data
	public static byte [] downloadPage(String url,String hdfsPath){
		CloseableHttpClient httpclient = HttpClients.createDefault();
		byte[] responseBody;
		HttpGet httpget = new HttpGet(url);
		try {
			CloseableHttpResponse response = httpclient.execute(httpget);
	        HttpEntity entity = response.getEntity();
	        int status = response.getStatusLine().getStatusCode();
	        if (status >= 200 && status < 300) {
	        	System.out.print(status);
            	// ok it seems that 

    			//conver the entity to byte data,we can regard the entity as page at this place
	        	responseBody = EntityUtils.toByteArray(entity);
    	        response.close();

    			return responseBody;
            }
	        else{
	        	responseBody=null;
	        	response.close();
	        	return null;
	        }
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
        //configuration of hadoop file system
		responseBody=null;
		return responseBody;
            
	}

}