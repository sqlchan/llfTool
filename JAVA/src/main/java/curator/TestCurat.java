package curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.midi.Soundbank;

public class TestCurat {
    private static final Logger log = LoggerFactory.getLogger(TestCurat.class);
    private static String address = "10.33.57.46:2181";
    private static String path = "llf";

    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(address)
                .sessionTimeoutMs(60000).connectionTimeoutMs(15000).namespace(path)
                .retryPolicy(retry).build();
        client.start();

        Stat existStat = client.checkExists().forPath("/node1");
        if (null == existStat){
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath("/node1","node1".getBytes());
            log.info("create node1");

        }else log.error("node1 exist");

        Stat statdata = new Stat();
        byte[] statdatabyte = client.getData().storingStatIn(statdata).forPath("/node1");
        System.out.println("node1 data: "+statdatabyte.toString());

        TreeCache treeCache = new TreeCache(client,"/node1");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("even type: "+event.getType()+", path: "+event.getData().getPath()+", data: "+event.getData().getData());
            }
        });
        treeCache.start();

        client.delete().deletingChildrenIfNeeded().forPath("/node1");
        Thread.sleep(5000);
    }
}
