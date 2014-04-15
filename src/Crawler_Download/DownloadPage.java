package Crawler_Download;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
	/*
	 */
	private void saveToLocal(byte[] data,String filePath)
	{
		try {
			DataOutputStream out=new DataOutputStream(
					new FileOutputStream(new File(filePath)));
			for(int i=0;i<data.length;i++){
				out.write(data[i]);
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//this time ,we just return the page data
	public static void downloadPage(String url,String hdfsPath)throws Exception{
		String fileHdfsPath=null;
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpGet httpget = new HttpGet(url);
		CloseableHttpResponse response = httpclient.execute(httpget);
        HttpEntity entity = response.getEntity();
        //configuration of hadoop file system
        Configuration conf = new Configuration();
        conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
       // String hdfsPath="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream out;
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
            	// ok it seems that 
            	
    			fileHdfsPath=hdfsPath+getFileNameByUrl(url,entity.getContentType().toString());
    		      if (fileSystem.exists(new Path(fileHdfsPath))) {
    		          System.out.println("File " + hdfsPath + " already exists");
    		          return;
    		      }
    	        out= fileSystem.create(new Path(fileHdfsPath));
    			//conver the entity to byte data,we can regard the entity as page at this place
    			byte[] responseBody = EntityUtils.toByteArray(entity);
    			out.write(responseBody);
    			out.close();
    			fileSystem.close();
            } else {
            	//some websites has forbidden the crawler,and if I have time ,I will fix this problem
            	fileHdfsPath = null;
            	if(status==403){
            		System.out.println("this "+url+"forbide crawler");
            	}
            	fileSystem.close();
                //throw new ClientProtocolException("Unexpected response status: " + status);
            }
            response.close();
	}

}