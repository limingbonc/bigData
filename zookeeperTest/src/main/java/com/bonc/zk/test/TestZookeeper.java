package com.bonc.zk.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2019/1/2 23:20
 */
public class TestZookeeper {

    private  ZooKeeper zkClient ;
    private String contectString="hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTime=2000;

    @Before
    public void init () throws IOException {
        zkClient =   new ZooKeeper(contectString, sessionTime, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
              /*  List<String> childrens = null;
                try {
                    System.out.println("------------start-----------");
                    childrens = zkClient.getChildren("/atguigu",true);
                    for (String child:childrens){
                        System.out.println(child);
                    }
                    System.out.println("-------------end---------------");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/

            }
        });
    }

    //1.创建子节点
    @Test  //连接服务器在操作之前完成
    public void createNode() throws KeeperException, InterruptedException {
      String result =   zkClient.create("/atguigu","lisan".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(result);
    }

    //2.监控子节点，并获取其中的数据;将监听代码写在连接的Process方法中
   @Test
    public void watchNode() throws KeeperException, InterruptedException {
     List<String> childrens = zkClient.getChildren("/atguigu",true);
     for (String child:childrens){
         System.out.println(child);
     }
     Thread.sleep(Long.MAX_VALUE); //让线程不要停，保持更新
    }

    //判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/idea",false);
        System.out.println(stat==null?"no exist":"exist");
    }
}
