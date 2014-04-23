package onfire.configure;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class configure {
	public static final String HTMLFILESPATH="hdfs://ubuntu:9000/Crawler/HtmlFiles/";
	public static final String URLFILESPATH="hdfs://ubuntu:9000/Crawler/UrlFiles/";
	public static final String HTMLFILESINFOPATH="hdfs://ubuntu:9000/Crawler/HtmlFileInfo/";
	public static final String URLNAME="/url.txt";
	public static final String HTMLINFONAME="/HtmlInfo.txt";
	public static final String getUrlPath(String level){
		return URLFILESPATH+level+URLNAME;
	}
	public static final String getHtmlPath(String level){
		return HTMLFILESPATH+level+"/";
	}
	public static final String getHtmlPathInfo(String level){
		return HTMLFILESINFOPATH+level+HTMLINFONAME;
	}
	public static final void createFile(String path) throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    if (fileSystem.exists(new Path(path))) {
	    	return ;
	    }
	    FSDataOutputStream out=fileSystem.create(new Path(path));
		out.close();
		fileSystem.close();
	}
	public static final String getNextHtmlFilePath() throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(HTMLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    if(listedPaths.length==0){
	    	System.out.println("warning,we have none the html file ");
	    }
	    int levelNumber=listedPaths.length;
	    levelNumber++;// you can think about why
	    String path=HTMLFILESPATH+Integer.toString(levelNumber)+"/";
	    fileSystem.close();
	    return path;
	}
	public static final String getLatestUnprocessedUrlFilePath() throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(URLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    String path=URLFILESPATH+Integer.toString(levelNumber)+URLNAME;
	    fileSystem.close();
		return path;
	}
	public static final String getNextUnprocessedUrlFilePath() throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(URLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    levelNumber++; 
	    String path=URLFILESPATH+Integer.toString(levelNumber)+URLNAME;
	    fileSystem.close();
		return path;
	}
	//
	public static final String getLatestMergedUrlFolderPath() throws IOException{
		//
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(URLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    String path=URLFILESPATH+Integer.toString(levelNumber)+"/merge/";
	    return path;
	}
	public static final ArrayList<String> getLatestMergedUrlFilePath() throws IOException{
		ArrayList<String> pathList=new ArrayList<String>();
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(URLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    String path=URLFILESPATH+Integer.toString(levelNumber)+"/merge/";
		if (!fileSystem.exists(new Path(path))) {
	    	System.out.println("Warning,the merge file does not exists");
			return pathList;
		}
	    FileStatus[] status2=fileSystem.listStatus(new Path(path));
	    Path[] listedPaths2 = FileUtil.stat2Paths(status2);
	    for(int i=0;i<listedPaths2.length;i++){
	    	if(!listedPaths2[i].toString().equals(path+"_SUCCESS")){
	    		pathList.add(listedPaths2[i].toString());
	    	}
	    }
	    fileSystem.close();
	    return pathList;
	}
	public static final String getLatestValidUrlFolderPath()throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(URLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    String path=URLFILESPATH+Integer.toString(levelNumber)+"/valid/";
	    return path;
		
	}
	public static final ArrayList<String>  getLatestValidUrlFilePath() throws IOException{
		ArrayList<String> pathList=new ArrayList<String>();
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(URLFILESPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    String path=URLFILESPATH+Integer.toString(levelNumber)+"/valid/";
		if (!fileSystem.exists(new Path(path))) {
	    	System.out.println("Warning,the valid file does not exists");
			return null;
		}
	    FileStatus[] status2=fileSystem.listStatus(new Path(path));
	    Path[] listedPaths2 = FileUtil.stat2Paths(status2);
	    if(listedPaths2.length==0){
	    	System.out.println("Warning!! we have none valid url files");
	    }
	    for(int i=0;i<listedPaths2.length;i++){
	    	if(!listedPaths2[i].toString().equals(path+"_SUCCESS")){
	    		pathList.add(listedPaths2[i].toString());
	    	}
	    }
	    fileSystem.close();
	    return pathList;
	}
	//
	public static final String getLatestHtmlFilePathInfo() throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(HTMLFILESINFOPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    if(listedPaths.length==0){
	    	System.out.println("warning ,there is no htmlfile info file exits");
	    }
	    int levelNumber=listedPaths.length;
	    String path=HTMLFILESINFOPATH+Integer.toString(levelNumber)+HTMLINFONAME;
	    fileSystem.close();
		return path;
	}
	public static final String getNextHtmlFilePathInfo() throws IOException{
		Configuration conf = new Configuration();
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/opt/hadoop-2.3.0/etc/hadoop/hdfs-site.xml"));
	    FileSystem fileSystem;
	    fileSystem = FileSystem.get(conf);
	    FileStatus[] status=fileSystem.listStatus(new Path(HTMLFILESINFOPATH));
	    Path[] listedPaths = FileUtil.stat2Paths(status);
	    int levelNumber=listedPaths.length;
	    levelNumber++;
	    String path=HTMLFILESINFOPATH+Integer.toString(levelNumber)+HTMLINFONAME;
	    fileSystem.close();
		return path;
	}
	
}
