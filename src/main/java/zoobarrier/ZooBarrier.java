package zoobarrier;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooBarrier {
    private String zkParam;
    private String root;
    private int sessionTimeout;
    private ZooKeeper zk=null;
    private CountDownLatch awaitCount;


    public ZooBarrier(String zkParam,String root,int sessionTimeout){
        this.sessionTimeout=sessionTimeout;
        this.root=root;
        try {
            zk = new ZooKeeper(zkParam, sessionTimeout,null);
            Stat stat = zk.exists(root, false);
            if (stat == null) {
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
