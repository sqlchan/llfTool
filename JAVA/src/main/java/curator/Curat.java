package curator;

import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

public class Curat {
    private static String path = "/llf/app2/consumer";
    public static void main(String[] args) throws Exception {
        String address = "10.33.57.46:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(address,new ExponentialBackoffRetry(1000,3));
        try {
            client.start();
            //创建永久节点
            client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/llf/yongjiu/test","llfyongjiu".getBytes());
            //创建临时节点
            //client.create().withMode(CreateMode.EPHEMERAL).forPath("/llf/linshi","llflinshi".getBytes());
            //获取数据
            System.out.println("path: "+path+" data: "+new String(client.getData().forPath("/llf/yongjiu")));
//            //设置数据
//            client.setData().inBackground().forPath("/llf/yongjiu/test","llfyongjiu11".getBytes());
//            //检查是否存在
//            Stat stat = client.checkExists().forPath("/llf/yongjiu");
//            if (null == stat) System.out.println("no path");
//            else System.out.println("path exist");
//            Stat stat1 = new Stat();
//            byte[] bytes = client.getData().storingStatIn(stat1).forPath("/llf/yongjiu");
//            System.out.println(JSON.toJSONString(stat1));
            //级联删除节点
            //client.delete().deletingChildrenIfNeeded().forPath("/llf/yongjiu1");

        }finally {
            client.close();
        }
    }
}
