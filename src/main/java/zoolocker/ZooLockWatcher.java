package zoolocker;
import com.sun.org.apache.xerces.internal.util.SynchronizedSymbolTable;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;


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
        if (this.awaitCount != null) {
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
                waitForLock(preLock,sessionTimeout);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if (lockName.contains(splitStr)) {
                throw new LockException("锁名有误");
            }

            currentLock = zk.create(root + "/" + lockName + splitStr, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(currentLock + " 已经创建");
            List<String> subNodes = zk.getChildren(root, false);
            List<String> lockObjects = new ArrayList<String>();
            for (String node : subNodes) {
                String _node = node.split(splitStr)[0];
                if (_node.equals(lockName)) {
                    lockObjects.add(node);
                }
            }
            Collections.sort(lockObjects);
            System.out.println(Thread.currentThread().getName() + " 的锁是 " + currentLock);
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
                System.out.println("success get the lock");
                return true;
            }
            return waitForLock(preLock, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 等待锁
    private boolean waitForLock(String prev, long waitTime) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(root + "/" + prev, true);

        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + "等待锁 " + root + "/" + prev);
            this.awaitCount = new CountDownLatch(1);
            // 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
            this.awaitCount.await(waitTime, TimeUnit.MILLISECONDS);
            this.awaitCount = null;
            System.out.println(Thread.currentThread().getName() + " 等到了锁");
        }
        return true;
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
