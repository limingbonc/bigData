package com.bonc.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/13 14:51
 */
public class FlowCountDriver  {

        public static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException {

            args = new String[]{"e:/input/inputflow","e:/output1"};

        Configuration conf = new Configuration();
        //1.创建jab
        Job job = Job.getInstance(conf);

        //2.设置jar存储位置
        job.setJarByClass(FlowCountDriver.class);
        //3.关联Map 和 reducer类
      job.setMapperClass(FlowCountMapper.class);
      job.setReducerClass(FlowCountReduce.class);
        //4.设置mapper阶段数据输出的key 和vaule 类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);

        // 5. 设置最终输出的key 和 value 类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

        //6. 设置输入和输出路径(注意导入的包)
           FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // .7 提交job
          Boolean result =   job.waitForCompletion(true);

          System.exit(result?0:1);
        }

}
