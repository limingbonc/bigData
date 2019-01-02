package com.bonc.zk.test;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

/**
 * @auther :liming
 * @Description:
 * @Date: create in 2019/1/2 23:20
 */
public class TestZookeeper {

    private  ZooKeeper zkClient ;
    private String contectString="hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTime=2000;

    @Test
    public void init () throws IOException {
        zkClient =   new ZooKeeper(contectString, sessionTime, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
