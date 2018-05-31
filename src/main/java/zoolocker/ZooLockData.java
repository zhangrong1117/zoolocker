package zoolocker;

public class ZooLockData {

    private int lockCount;
    private Thread lockThread;

    public ZooLockData(int lockCount,Thread lockThread) {
        this.lockCount = lockCount;
        this.lockThread = lockThread;
    }

    public boolean increaseCount(){
        this.lockCount+=1;
        return true;
    }

    public boolean decreaseCount(){
        if(this.lockCount>1){
            this.lockCount-=1;
            return true;
        }
        return false;

    }

}
