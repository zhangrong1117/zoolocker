import org.I0Itec.zkclient.*;


public class ZooLockerMain {
     public static void main(String []args){
        String zkServers ="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        ZkClient zkClient=new ZkClient(zkServers);
        zkClient.createEphemeral("/zookeeper/node8","testdzrtest");
        while (true){

        }


    }
}
