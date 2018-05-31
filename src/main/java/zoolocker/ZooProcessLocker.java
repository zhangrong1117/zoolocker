package zoolocker;

import sun.jvm.hotspot.debugger.ThreadAccess;
import java.util.concurrent.*;
import java.util.*;


public class ZooProcessLocker {

    private ConcurrentHashMap<Thread,ZooLockWatcher> ThreadMap=new ConcurrentHashMap<Thread, ZooLockWatcher>();
    private String zkParam;
    private String root;
    private String lockName;
    private int sessionTimeout;


    public ZooProcessLocker(String zkParam,String root,String lockName,int sessionTimeout){
        this.zkParam=zkParam;
        this.root=root;
        this.lockName=lockName;
        this.sessionTimeout=sessionTimeout;
    }

    public void realase(){
       ZooLockWatcher zooLockWatcher=ThreadMap.get(Thread.currentThread());
       if (!zooLockWatcher.getLockData().decreaseCount()){
           zooLockWatcher.unlock();
           ThreadMap.remove(Thread.currentThread());
       }
    }

    public boolean acquire(){
        if(ThreadMap.containsKey(Thread.currentThread())){
           ZooLockWatcher zooLockWatcher=ThreadMap.get(Thread.currentThread());
           zooLockWatcher.getLockData().increaseCount();
        }
        else{
            ZooLockWatcher zooLockWatcher=new ZooLockWatcher(zkParam,sessionTimeout,root,lockName);
            zooLockWatcher.setLockData(new ZooLockData(1,Thread.currentThread()));
            ThreadMap.put(Thread.currentThread(),zooLockWatcher);
            zooLockWatcher.tryLock();


        }
        return true;
    }
}
