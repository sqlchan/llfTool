package curator.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class Test {
    public static void main(String[] args) throws Exception {
        String connectionString = "127.0.0.1";
        /**
         * 随着重试次数增加重试时间间隔变大,指数倍增长baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)))。
         * 有两个构造方法
         * baseSleepTimeMs初始sleep时间
         * maxRetries最多重试几次
         * maxSleepMs最大的重试时间
         * 如果在最大重试次数内,根据公式计算出的睡眠时间超过了maxSleepMs，将打印warn级别日志,并使用最大sleep时间。
         * 如果不指定maxSleepMs，那么maxSleepMs的默认值为Integer.MAX_VALUE。
         */
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000,3,1000);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        curatorFramework.start();

        CuratorFramework curatorFramework1 = CuratorFrameworkFactory.newClient(connectionString, 60000, 15000, retryPolicy);
        curatorFramework1.start();

        /**
         * connectionString zk地址
         * sessionTimeoutMs 会话超时时间
         * connectionTimeoutMs 连接超时时间
         * namespace 每个curatorFramework 可以设置一个独立的命名空间,之后操作都是基于该命名空间，比如操作 /app1/message 其实操作的是/node1/app1/message
         * retryPolicy 重试策略
         */
        CuratorFramework curatorFramework2 = CuratorFrameworkFactory.builder().connectString(connectionString)
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(15000)
                .namespace("/node1")
                .retryPolicy(retryPolicy)
                .build();
        curatorFramework2.start();

        //=========================创建节点=============================

        /**
         * 创建一个 允许所有人访问的 持久节点
         */
        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .forPath("/node10/child_01","123456".getBytes());

        //=========================获取节点=============================

        /**
         * 获取节点 /node10/child_01 数据 和stat信息
         */
        Stat node10Stat = new Stat();
        byte[] node10 = curatorFramework.getData()
                .storingStatIn(node10Stat)//获取stat信息存储到stat对象
                .forPath("/node10/child_01");
        System.out.println("=====>该节点信息为：" + new String(node10));
        System.out.println("=====>该节点的数据版本号为：" + node10Stat.getVersion());

        /**
         * 获取节点信息 并且留下 Watcher事件 该Watcher只能触发一次
         */
        byte[] bytes = curatorFramework.getData()
                .usingWatcher(new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("=====>wathcer触发了。。。。");
                        System.out.println(event);
                    }
                })
                .forPath("/node10/child_01");
        System.out.println("=====>获取到的节点数据为："+new String(bytes));
        Thread.sleep(1000);

        // NodeCache: 对一个节点进行监听，监听事件包括指定的路径节点的增、删、改的操作。
        // PathChildrenCache: 对指定的路径节点的一级子目录进行监听，不对该节点的操作进行监听，对其子目录的节点进行增、删、改的操作监听
        // TreeCache:  可以将指定的路径节点作为根节点（祖先节点），对其所有的子节点操作进行监听，呈现树形目录的监听，可以设置监听深度，最大监听深度为2147483647（int类型的最大值）。
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework,"/llf/yongjiu",true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("even type: "+event.getType()+", path: "+event.getData().getPath()+", data: "+event.getData().getData());
                List<ChildData> list = pathChildrenCache.getCurrentData();
                if (null != list && list.size()> 0){
                    for (ChildData childData: list){
                        System.out.println("path: "+ childData.getPath()+", data: "+ childData.getData());
                    }
                }
            }
        });

        pathChildrenCache.start();

        //=========================删除节点=============================

        /**
         * 删除node节点 不递归  如果有子节点,将报异常
         */
        Void aVoid = curatorFramework.delete()
                .forPath("/node10");
        System.out.println("=====>" + aVoid);

        /**
         * 递归删除,如果有子节点 先删除子节点
         */
        curatorFramework.delete()
                .deletingChildrenIfNeeded()
                .forPath("/node10");

        curatorFramework.close();

        /**
         * 删除
         */
        curatorFramework.delete()
                .guaranteed()
                .forPath("/node11");

        //=========================判断节点是否存在=============================

        /**
         * CuratorFramework类有一个判断节点是否存在的接口checkExists()，该接口返回一个org.apache.zookeeper.data.Stat对象，对象中有一个ephemeralOwner属性。
         * 如果该节点是持久化节点，ephemeralOwner的值为0
         * 如果该节点是临时节点，ephemeralOwner的值大于0
         */

        Stat existsNodeStat = curatorFramework.checkExists().forPath("/node10");
        if(existsNodeStat == null){
            System.out.println("=====>节点不存在");
        }
        if(existsNodeStat.getEphemeralOwner() > 0){
            System.out.println("=====>临时节点");
        }else{
            System.out.println("=====>持久节点");
        }

    }
}
