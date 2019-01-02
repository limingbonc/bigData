package com.bonc.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/12 10:50
 */
// map阶段
// KEYIN 输入数据的key
// VALUEIN 输入数据的value
// KEYOUT 输出数据的key的类型   atguigu,1   ss,1
// VALUEOUT 输出的数据的value类型
public class WordCountMapper  extends Mapper<LongWritable,Text,Text,IntWritable>{

    //避免过多的创建对象，进行提取
    Text k = new Text();
    IntWritable v = new IntWritable(1);


    //重写map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(key.toString());

        //1.读取一行数据
        String  line =  value.toString();

        //2.对于存在空格的进行拆分
        String[] words = line.split(" ");

        for(String word :words ){
              k.set(word); //对key进行赋值，value是 1
        }
        context.write(k,v);

    }
}
