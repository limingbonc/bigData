package com.bonc.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/29 15:08
 */
public class TwoIndexReducer  extends Reducer<Text,Text,Text,Text>{

    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //     atguigu  a.txt	3
        StringBuffer sb= new StringBuffer();
        for(Text value:values){
            sb.append(value.toString().replaceAll("\t","-->")+"\t");
        }

        v.set(sb.toString());
        context.write(key,v);
    }
}
