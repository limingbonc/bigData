package com.bonc.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/16 11:29
 */
public class NLineMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        // 2 切割
        String[] words = line.split(" ");

        // 3 循环写出
        for (String word : words) {

            k.set(word);

            context.write(k, v);
        }
    }
}
