package com.bonc.mr.inputformat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/17 22:31
 */
public class SequenceFileMapper  extends Mapper<Text,BytesWritable,Text,BytesWritable>{
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
