package zoolocker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.AtomicInteger;


public class ZooLockWatcher implements  Watcher,Lock {
    private ZooKeeper zk = null;  //zk客户端
    private String root;     //锁的根路径
    private String lockName;
    private String currentLock; //当前线程创建节点
    private String preLock = null;  // 优先级高于当前节点一级的节点
    private CountDownLatch awaitCount; //阻塞计数器
    private List<Exception> exceptionList = new ArrayList<Exception>();
    private int sessionTimeout;
    private ZooLockData lockData=new ZooLockData(0,Thread.currentThread());


    public ZooLockWatcher(String zkParam, int sessionTimeout, String root,String lockName) {
        this.lockName=lockName;
        this.root=root;
        this.sessionTimeout=sessionTimeout;
        try {
            zk = new ZooKeeper(zkParam, sessionTimeout, this);
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

    public void process(WatchedEvent event) {
        if (event.getType() != Event.EventType.NodeDeleted) {
            throw new LockException("非法操作");
        }
        List<String> lockObjects = getLockNodes("_lock_");
        Collections.sort(lockObjects);
        if (!currentLock.equals(root + "/" + lockObjects.get(0))) {
            String prevNode = currentLock.substring(currentLock.lastIndexOf("/") + 1);
            preLock = lockObjects.get(Collections.binarySearch(lockObjects, prevNode) - 1);
            try {
                Stat stat = zk.exists(root + "/" + preLock, true);
                if (stat == null) {
                    this.awaitCount.countDown();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        } else if (this.awaitCount != null) {
            this.awaitCount.countDown();
        }
    }

    public void lock() {
        if (exceptionList.size() > 0) {
            throw new LockException(exceptionList.get(0));
        }
        try {
            if (this.tryLock()) {
                return;
            } else {
                wait(preLock,sessionTimeout);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public boolean tryLock() {
        try {
            String defineLock = "_lock_";
            currentLock = zk.create(root + "/" + lockName + defineLock, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            List<String> lockObjects=getLockNodes(defineLock);
            Collections.sort(lockObjects);
            if (currentLock.equals(root+ "/" + lockObjects.get(0))) {
                return true;
            }

            String prevNode = currentLock.substring(currentLock.lastIndexOf("/") + 1);
            preLock= lockObjects.get(Collections.binarySearch(lockObjects, prevNode) - 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean tryLock(long timeout, TimeUnit unit) {
        try {
            if (this.tryLock()) {
                return true;
            }
            return wait(preLock, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    private boolean wait(String prev, long waitTime) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(root + "/" + prev, true);

        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + "等待锁 " + root + "/" + prev);
            this.awaitCount = new CountDownLatch(1);
            this.awaitCount.await(waitTime, TimeUnit.MILLISECONDS);
            this.awaitCount = null;
            System.out.println(Thread.currentThread().getName() + " 等到了锁");
        }
        return true;
    }

    public List<String> getLockNodes(String defineLock){

        try {
            List<String> subNodes = zk.getChildren(root, false);
            List<String> lockObjects = new ArrayList<String>();
            for (String node : subNodes) {
                String _node = node.split(defineLock)[0];
                if (_node.equals(lockName)) {
                    lockObjects.add(node);
                }
            }
            return lockObjects;
        }catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void unlock() {
        try {
            zk.delete(currentLock, -1);
            currentLock = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public Condition newCondition() {
        return null;
    }

    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }


    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }

    public ZooLockData getLockData() {
        return lockData;
    }

    public void setLockData(ZooLockData lockData) {
        this.lockData = lockData;
    }
}
