package com.bonc.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/18 23:11
 */
public class FlowCountSortReducer  extends Reducer<FlowBean,Text,Text,FlowBean>{

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		13736230513	2481	24681	27162

        for (Text value : values) {
            context.write(value, key); //在map中key 是bean,手机号是value， 因此 输出时反过来
        }
    }

}
