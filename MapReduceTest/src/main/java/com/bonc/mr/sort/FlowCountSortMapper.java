package com.bonc.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/18 23:11
 */
public class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
    Text v = new Text();
    FlowBean k = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line  =  value.toString();
         String[] fields = line.split("\t");

        String phoneNbr = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);
        v.set(phoneNbr);

        // 4 输出
        context.write(k, v);

    }
}
