package Crwaler_Download;

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

public class DownloadPage {
	private String getFileNameByUrl(String url,String contentType)
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
	public void downloadPage(String url)throws Exception{
		String filePath=null;
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpGet httpget = new HttpGet(url);
		CloseableHttpResponse response = httpclient.execute(httpget);
        HttpEntity entity = response.getEntity();
		try {
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
            	// ok it seems that 
    			filePath="/home/ubuntu/test/temp/"+getFileNameByUrl(url,entity.getContentType().toString());
    			//conver the entity to byte data,we can regard the entity as page at this place
    			byte[] responseBody = EntityUtils.toByteArray(entity);
    			saveToLocal(responseBody,filePath);
            } else {
            	//some websites has forbidden the crawler,and if I have time ,I will fix this problem
            	filePath = null;
            	if(status==403){
            		System.out.println("this"+url+"forbide crawler");
            	}
                //throw new ClientProtocolException("Unexpected response status: " + status);
            }
		} finally {
		    response.close();
		}
	}

}