package com.bonc.mr.kvtext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/14 15:52
 */
public class KVTextDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"e:/input/kvinput","e:/output"};

        Configuration conf = new Configuration();
        //设置分隔符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        //1.创建job对象
        Job  job =   Job.getInstance(conf);
        //2.设置jar存储路径
        job.setJarByClass(KVTextDriver.class);
        //3.关联mapper 和Reduce
    job.setMapperClass(KvTextMapper.class);
    job.setReducerClass(KvTextReducer.class);
        //4.设置map 输出类型  key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //5.设置最终key ，value输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //6. 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7.提交job
     boolean  result =  job.waitForCompletion(true);
     System.exit(result?0:1);

    }

}
