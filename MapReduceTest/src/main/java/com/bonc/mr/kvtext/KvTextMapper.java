package com.bonc.mr.kvtext;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/14 15:39
 */
public class KvTextMapper extends Mapper<Text, Text, Text, LongWritable>{

    // 1 设置value
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

// banzhang ni hao

        // 2 写出
        context.write(key, v);
    }
}
