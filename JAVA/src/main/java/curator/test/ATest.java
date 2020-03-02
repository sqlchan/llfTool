package curator.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ATest {
    public static void main(String[] args) throws Exception {

        String connectionString = "192.168.58.42:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        curatorFramework.start();

        //=========================创建节点=============================

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("===>响应码：" + event.getResultCode()+",type:" + event.getType());
                        System.out.println("===>Thread of processResult:"+Thread.currentThread().getName());
                        System.out.println("===>context参数回传：" + event.getContext());
                    }
                },"传给服务端的内容,异步会传回来", executorService)
                .forPath("/node10","123456".getBytes());
        Thread.sleep(3000);


        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("===>响应码：" + event.getResultCode()+",type:" + event.getType());
                        System.out.println("===>Thread of processResult:"+Thread.currentThread().getName());
                        System.out.println("===>context参数回传：" + event.getContext());
                    }
                },"传给服务端的内容,异步会传回来")
                .forPath("/node10","123456".getBytes());
        Thread.sleep(3000);
        executorService.shutdown();

                //开始事务操作
//                CuratorOp createParentNode = client.transactionOp().create().forPath("/a", "some data".getBytes());
//                CuratorOp createChildNode = client.transactionOp().create().forPath("/a/path", "other data".getBytes());
//                CuratorOp setParentNode = client.transactionOp().setData().forPath("/a", "other data".getBytes());
//                CuratorOp deleteParent = client.transactionOp().delete().forPath("/a");
//
//                Collection<CuratorTransactionResult> results = client.transaction().forOperations(createParentNode, createChildNode, setParentNode,deleteParent);


    }

}
