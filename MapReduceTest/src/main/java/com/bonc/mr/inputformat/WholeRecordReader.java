package com.bonc.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/17 22:00
 */
public class WholeRecordReader extends RecordReader<Text,BytesWritable>{
    FileSplit split;
    Configuration configuration;
    Text  k = new Text();
    BytesWritable v = new BytesWritable();

    boolean isProgress = true;
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) split;
         configuration =  context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
       //核心业务逻辑处理
        if (isProgress){
            // 1 定义缓存区
         byte[] contents = new byte[(int) split.getLength()];
            //2 获取文件系统
            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            // 3 读取数据
          FSDataInputStream fis = fileSystem.open(path);
            // 4 读取文件内容
            IOUtils.readFully(fis,contents,0,contents.length);

            // 5 输出文件内容
             v.set(contents,0,contents.length);
            // 6 获取文件路径及名称
            String name = split.getPath().toString();
            // 7 设置输出的key值
             k.set(name);//key是文件路径及名称

            isProgress = false;
            return true;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
