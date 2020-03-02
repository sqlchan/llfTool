package zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Listener {
    private static final Logger logger = LoggerFactory.getLogger(Listener.class);
    private static String address = "10.33.57.46:2181";

    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework client = CuratorFrameworkFactory.newClient(address,retry);
        client.start();
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/llf");
        List<String> list = client.getChildren().forPath("/llf");
        System.out.println(list.toString());
//        PathChildrenCache cache = new PathChildrenCache(client,"/llf",true);
//
//        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
//        cache.getListenable().addListener((client1, event) -> {
//            switch (event.getType()){
//                case CHILD_ADDED:
//                    System.out.println("add: "+ event.getData().getPath()+", data: "+new String(event.getData().getData(),"UTF-8").toString());
//                    break;
//                case CHILD_REMOVED:
//                    System.out.println("remove: "+event.getData().getPath());
//                    break;
//                case CHILD_UPDATED:
//                    System.out.println("updated: "+event.getData().getPath());
//                    break;
//                default:
//                    break;
//            }
//        });
        Thread.sleep(300000);
    }
}
