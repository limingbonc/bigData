package com.bonc.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/25 21:58
 * 整个服务先走mapper，再走Combiner，然后Reducer
 */
public class WordcountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value:values ){
            sum+= value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
