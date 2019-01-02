package com.bonc.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/13 11:02
 */
public class FlowCountMapper  extends Mapper<LongWritable,Text,Text,FlowBean>{

    Text k = new Text();
    FlowBean v = new FlowBean();
    //重写map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             //数据样例
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        //2	13846544121	192.196.100.2			        264    	0	    200

        //1.获取一行
      String line =  value.toString();
        //2.进行切割
           String[]  flieds =   line.split("\t");

            //3.封装对象
            k.set(flieds[1]);// 封装手机号

           Long upFlow = Long.parseLong( flieds[flieds.length-3]) ;   //由于原数据存在空，所以从后往前进行截取
           Long  downFlow = Long.parseLong(flieds[flieds.length-2]);  //由于原数据存在空，所以从后往前进行截取

           v.setUpFlow(upFlow);
           v.setDownFlow(downFlow);

        //4.写出
        context.write(k,v);
    }
}
