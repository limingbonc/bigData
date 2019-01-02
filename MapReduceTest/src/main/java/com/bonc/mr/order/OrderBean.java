package com.bonc.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2018/12/25 22:21
 */
public class OrderBean implements WritableComparable<OrderBean>{
    private int order_id; //订单id
    private Double price;  //订单价格



    public OrderBean(int order_id, Double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public OrderBean() {
    }

    public int compareTo(OrderBean o) {
        int result ;
        //先按id进行升序，然后按价格进行降序
        if(order_id>o.getOrder_id()){
            result = 1;
        }else if(order_id<o.getOrder_id()){
            result =-1;
        }else{
            if(price>o.getPrice()){
                result =-1;
            }else if(price<o.getPrice()){
                result =1;
            }else {
                result =0;
            }
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
          dataOutput.writeInt(order_id);
          dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
         order_id = dataInput.readInt();
         price =dataInput.readDouble();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return  order_id + "\t"+price;
    }
}
