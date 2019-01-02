package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/11/26 22:27
 */
public class HdfsCilent {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();

        //1.获取客户端对象
        FileSystem fs =  FileSystem.get(new URI("hdfs://192.168.253.102:9000"),conf,"atguigu");
        //2.进行操作
          fs.mkdirs(new Path("/user/atguigu/lm"));
        //3.关闭资源
        fs.close();

        System.out.println("over");
    }


    //判断是否是文件或者文件夹
    @Test
    public void testIfFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.253.102:9000"),conf,"atguigu");

          FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

          for(FileStatus fileStatus:fileStatuses){
              if(fileStatus.isFile()){
                  System.out.println("f:"+fileStatus.getPath().getName()); //输出文件名
              }else {
                  System.out.println("d:"+fileStatus.getPath().getName());//输出文件夹
              }
          }

    }
}
