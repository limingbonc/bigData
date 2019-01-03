package com.bonc.zk.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2019/1/3 11:55
 */
public class DistributeServer {

    private String contectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTime = 2000;
    private  ZooKeeper zkClient;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer server  = new DistributeServer();
        
        //1连接服务器
        server.getContect();
        // 2.注册监听
        server.registNode(args[0]);
        //3.业务逻辑处理
        server.business();

    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void registNode(String hostname) throws KeeperException, InterruptedException {
       String result =  zkClient.create("/servers/server",hostname.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"is online");
    }
    private void getContect() throws IOException {
     zkClient  =  new ZooKeeper(contectString, sessionTime, new Watcher() {
         public void process(WatchedEvent watchedEvent) {

         }
      });
    }

}
