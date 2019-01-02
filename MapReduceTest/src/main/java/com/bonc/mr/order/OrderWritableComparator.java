package com.bonc.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/25 22:18
 */
public class OrderWritableComparator extends WritableComparator {
    protected OrderWritableComparator() {
        super(OrderBean.class,true); //这里必须使用true，不然会报异常
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int result;
        // 要求只要id相同，就认为是相同的key
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        if(aBean.getOrder_id()>bBean.getOrder_id()){
            result = 1;
        }else if(aBean.getOrder_id()<bBean.getOrder_id()){
            result  = -1;
        }else{
            result =0;
        }
        return result;
    }
}
