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
	   * ���� CG
	   * ʱ�� 2019/5/24 14:43
	   * ע�� ����Ҫ������������
	  * ע������
	  * ���û��������Ժ�����ȡ��������ʧ��(���������)
	   */
	@Test
	public void updateFileAdmin(){
		try {
			System.out.println(System.getenv("HADOOP_HOME") );
			//0 ��ȡ������Ϣ
			Configuration configuration = new Configuration();
			//1 ��ȡ�ļ�ϵͳ
			FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
			// 2 �����������ݵ���Ⱥ
			fs.copyFromLocalFile(new Path("e:/weixin.sql"), new Path("/usr/solace/weixin.sql"));
			// 3 �ر�fs
			fs.close();
			System.out.println("over");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	 /**
	   * ���� CG
	   * ʱ�� 2019/5/24 14:43
	   * ע�� ��Ҫ������������
	   */
	@Test
	public void updateFile(){
		/**
		 * ��Ҫ���û�������
		 * -DHADOOP_USER_NAME=root
		 */
		try {
			//0 ��ȡ������Ϣ
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://hadoop01:9000");
			//1 ��ȡ�ļ�ϵͳ
			FileSystem fs = FileSystem.get(configuration);
			// 2 �����������ݵ���Ⱥ
			fs.copyFromLocalFile(new Path("e:/weixin.sql"), new Path("/usr/solace/weixin.sql"));
			// 3 �ر�fs
			fs.close();
			System.out.println("over");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void getFileSystem() throws Exception{
		// 0 ����������Ϣ����
		Configuration configuration = new Configuration();
		// 1 ��ȡ�ļ�ϵͳ
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "root");
		// 2��ӡ�ļ�ϵͳ
		System.out.println(fs.toString());
		// 3 �ر���Դ
		fs.close();
	}
}
