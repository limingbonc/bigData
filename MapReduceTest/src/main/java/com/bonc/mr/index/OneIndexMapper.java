package com.bonc.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/29 11:13
 */
public class OneIndexMapper  extends Mapper<LongWritable,Text,Text,IntWritable> {
    String name;
    Text k =  new Text();
    IntWritable v = new IntWritable();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
     //获取文件名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        //atguigu pingping
       String line =  values.toString();
      String[] fileds = line.split(" ");
         for(String  word :fileds){
             k.set(word+"----"+name);
             v.set(1);
             context.write(k,v);
         }
    }
}
