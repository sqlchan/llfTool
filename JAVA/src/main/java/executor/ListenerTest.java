package executor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.TimeUnit;

import java.util.List;

// Path Cache可以监控ZNode子结点的变化，例如：add,update,delete
public class ListenerTest {
    public static void main(String[] args) throws Exception {
        String address = "10.33.57.46:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(address,new ExponentialBackoffRetry(1000,3));
        try {
            client.start();
            final PathChildrenCache pathChildrenCache = new PathChildrenCache(client,"/llf/yongjiu",true);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    System.out.println("cache child change");
                    System.out.println("even type: "+event.getType()+", path: "+event.getData().getPath()+", data: "+event.getData().getData());
                    List<ChildData> list = pathChildrenCache.getCurrentData();
                    if (null != list && list.size()> 0){
                        for (ChildData childData: list){
                            System.out.println("path: "+ childData.getPath()+", data: "+  new String(childData.getData()));
                        }
                    }
                }
            });

            pathChildrenCache.start();
            TimeUnit.MINUTES.sleep(5);
            pathChildrenCache.close();
        }finally {
            client.close();
        }
    }
}
