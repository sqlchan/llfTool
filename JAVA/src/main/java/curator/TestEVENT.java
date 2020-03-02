package curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestEVENT {
    private static final Logger log = LoggerFactory.getLogger(TestCurat.class);
    private static String address = "10.33.57.46:2181";
    private static String path = "llf";
    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(address)
                .sessionTimeoutMs(60000).connectionTimeoutMs(15000).namespace(path)
                .retryPolicy(retry).build();
        client.start();

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("asy ok");
                    }
                },"a go back",executorService).forPath("/node2","1234".getBytes());
        Thread.sleep(5000);

        InterProcessMutex interProcessMutex = new InterProcessMutex(client,"/node2");
        interProcessMutex.acquire();
        System.out.println(System.currentTimeMillis());
        interProcessMutex.release();

        LeaderLatch leaderLatch = new LeaderLatch(client,"/lead");
        leaderLatch.start();
        Thread.sleep(3000);
        System.out.println(leaderLatch.hasLeadership());
        leaderLatch.close();
    }
}
