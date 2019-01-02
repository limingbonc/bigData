package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/11/29 22:15
 */
public class HdfsIo {
    @Test
    public void putIoFileToHdfs() throws URISyntaxException, IOException, InterruptedException {

        //创建连接
        Configuration conf = new Configuration();

        FileSystem fs =  FileSystem.get(new URI("hdfs://192.168.253.102:9000"),conf,"atguigu");
        //输入流
        FileInputStream fis = new FileInputStream(new File("e:/lm.txt"));
        //s输出流
        FSDataOutputStream fos = fs.create(new Path("/lm.txt"));
        //流的对拷
        IOUtils.copyBytes(fis,fos,conf);

        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    @Test
    public void pullFileFromHdfs() throws URISyntaxException, IOException, InterruptedException {
        //创建连接
        Configuration conf = new Configuration();

        FileSystem fs =  FileSystem.get(new URI("hdfs://192.168.253.102:9000"),conf,"atguigu");

       FSDataInputStream fis =  fs.open(new Path("/lm.txt"));

      FileOutputStream fos = new  FileOutputStream(new File("e:/lmshuai.txt"));

      IOUtils.copyBytes(fis,fos,conf);

      IOUtils.closeStream(fos);
      IOUtils.closeStream(fis);
      fs.close();
    }
}
