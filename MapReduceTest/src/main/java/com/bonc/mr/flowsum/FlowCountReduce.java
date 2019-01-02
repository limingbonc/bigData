package com.bonc.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/13 11:45
 */
public class FlowCountReduce  extends Reducer<Text,FlowBean,Text,FlowBean>{

   FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow =0;

        //对于传进来的数据，可能存在key相同的，就需要进行遍历而累加求和
        for(FlowBean  value:values){
       sum_upFlow += value.getUpFlow();
       sum_downFlow +=value.getDownFlow();
        }

        v.set(sum_upFlow,sum_downFlow);
        context.write(key,v);
    }

}
