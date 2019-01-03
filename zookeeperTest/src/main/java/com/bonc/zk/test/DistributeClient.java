package com.bonc.zk.test;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2019/1/3 15:02
 */
public class DistributeClient {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
   DistributeClient client =new DistributeClient();
        //1.创建连接
        client.contect();
        //2.注册监听
        client.getChildren();
        //3.业务处理
        client.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);//保证实时打印
    }

    private String   contectString ="hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private  int sessionTime=2000;
    private ZooKeeper zooKeeper;
    private void contect() throws IOException {
        zooKeeper  =  new ZooKeeper(contectString, sessionTime, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren(); //目的是永远创建监听
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/servers", true);

        // 存储服务器节点主机名称集合
        List<String> hosts = new ArrayList<String>();

        for(String child :children){
            byte[] data = zooKeeper.getData("/servers/" + child, false, null); //获取节点数据
            hosts.add(new String(data));
        }
        // 将所有在线主机名称打印到控制台
        System.out.println(hosts);
    }
}
