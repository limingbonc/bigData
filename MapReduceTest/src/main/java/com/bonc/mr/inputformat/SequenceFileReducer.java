package com.bonc.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/17 22:33
 */
public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable>{
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
         context.write(key,values.iterator().next());
    }
}
