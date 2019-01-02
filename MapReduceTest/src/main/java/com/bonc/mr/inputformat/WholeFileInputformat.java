package com.bonc.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/17 21:58
 */
public class WholeFileInputformat  extends FileInputFormat<Text,BytesWritable>{
    WholeRecordReader recordReader = new WholeRecordReader();
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        recordReader.initialize(split, context);
        return recordReader;
    }
}
