package com.solace.hadoop_client_boot.hadoop.client;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {

	public static void main(String[] args) throws Exception {

	}
	 /**
	   * 作者 CG
	   * 时间 2019/5/24 14:43
	   * 注释 不需要设置启动参数
	  * 注意事项
	  * 设置环境变量以后程序获取环境变量失败(重启计算机)
	   */
	@Test
	public void updateFileAdmin(){
		try {
			System.out.println(System.getenv("HADOOP_HOME") );
			//0 获取配置信息
			Configuration configuration = new Configuration();
			//1 获取文件系统
			FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
			// 2 拷贝本地数据到集群
			fs.copyFromLocalFile(new Path("e:/weixin.sql"), new Path("/usr/solace/weixin.sql"));
			// 3 关闭fs
			fs.close();
			System.out.println("over");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	 /**
	   * 作者 CG
	   * 时间 2019/5/24 14:43
	   * 注释 需要设置启动参数
	   */
	@Test
	public void updateFile(){
		/**
		 * 需要设置环境变量
		 * -DHADOOP_USER_NAME=root
		 */
		try {
			//0 获取配置信息
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://hadoop01:9000");
			//1 获取文件系统
			FileSystem fs = FileSystem.get(configuration);
			// 2 拷贝本地数据到集群
			fs.copyFromLocalFile(new Path("e:/weixin.sql"), new Path("/usr/solace/weixin.sql"));
			// 3 关闭fs
			fs.close();
			System.out.println("over");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void getFileSystem() throws Exception{
		// 0 创建配置信息对象
		Configuration configuration = new Configuration();
		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
		// 2打印文件系统
		System.out.println(fs.toString());
		// 3 关闭资源
		fs.close();
	}
}
