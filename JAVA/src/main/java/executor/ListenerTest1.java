package executor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ListenerTest1 {
    public static void main(String[] args) throws Exception {
        String address = "10.33.57.46:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(address,new ExponentialBackoffRetry(1000,3));
        try {
            client.start();
            /*Curator之nodeCache一次注册，N次监听*/
            //为节点添加watcher
            //监听数据节点的变更，会触发事件
            final NodeCache nodeCache = new NodeCache(client,"/llf/yongjiu");
            //buildInitial: 初始化的时候获取node的值并且缓存
            nodeCache.start(true);
            if(nodeCache.getCurrentData() != null){
                System.out.println("节点的初始化数据为："+new String(nodeCache.getCurrentData().getData()));
            }else{
                System.out.println("节点初始化数据为空。。。");
            }

            nodeCache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    //获取当前数据
                    String data = new String(nodeCache.getCurrentData().getData());
                    System.out.println("节点路径为："+nodeCache.getCurrentData().getPath()+" 数据: "+data);
                }
            });


            //TimeUnit.MINUTES.sleep(5);


        }finally {
            client.close();
        }
    }
}
