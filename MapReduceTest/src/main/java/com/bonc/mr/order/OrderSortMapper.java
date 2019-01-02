package com.bonc.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/25 22:38
 */
public class OrderSortMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
    OrderBean bean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

      String[] fileds =  line.split("\t");
      bean.setOrder_id(Integer.parseInt(fileds[0]));
      bean.setPrice(Double.parseDouble(fileds[2]));

      context.write(bean,NullWritable.get());
    }
}
