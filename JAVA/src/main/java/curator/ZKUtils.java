package curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class ZKUtils implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZKUtils.class);

    private static volatile ZKUtils _INSTENCE = null;
    private final static byte[] lock = {};
    private CuratorFramework client;

    private static final int ZK_CONNECTION_TIMEOUT_MILLIS = 15000;
    // zk超时时间
    private static final int ZK_SESSION_TIMEOUT_MILLIS = 60000;
    private static final int RETRY_WAIT_MILLIS = 5000;
    // 出错尝试次数
    private static final int MAX_RECONNECT_ATTEMPTS = 3;


    /**
     * 初始化
     */
    private ZKUtils() {
        if (null == _INSTENCE) {
            /**
             * curator实现zookeeper锁服务
             */
            try {
                RetryPolicy retryPolicy = new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS);
                client = CuratorFrameworkFactory.builder().connectString(getZkHost()).retryPolicy(retryPolicy)
                        .sessionTimeoutMs(ZK_SESSION_TIMEOUT_MILLIS)
                        .connectionTimeoutMs(ZK_CONNECTION_TIMEOUT_MILLIS).build();

                client.start();
            } catch (Exception e) {
            }

        }
    }

    /**
     * zk地址拼接
     *
     * @return
     * @throws Exception
     */
    private String getZkHost() throws Exception {
        String hbaseZK = "";
        String port = "";

        StringBuilder zkHost = new StringBuilder();
        for (String host : hbaseZK.split(",")) {
            zkHost.append(host).append(":").append(port).append(",");
        }

        return zkHost.substring(0, zkHost.length() - 1).toString();
    }

    /**
     * zk工具类单例获取
     *
     * @return
     */
    public static ZKUtils getInstence() {
        if (_INSTENCE == null) {
            synchronized (lock) {
                if (_INSTENCE == null) {
                    _INSTENCE = new ZKUtils();
                }
            }
        }
        return _INSTENCE;
    }

    /**
     * 客户端操作句柄
     *
     * @return
     */
    public CuratorFramework getClient() {
        return client;
    }


    /**
     * 创建Zookeeper持久化节点,支持递归创建
     *
     * @param path
     * @param data
     * @throws Exception
     */
    public void create(String path, byte[] data) throws Exception {
        ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), path);
        client.setData().forPath(path, data);
    }

    /**
     * 创建Zookeeper节点并制定创建模式,不支持递归创建
     *
     * @param path
     * @param data
     * @param mode
     * @throws Exception
     */
    public void create(String path, byte[] data, CreateMode mode) throws Exception {
        try {
            client.create().withMode(mode).forPath(path, data);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 检查指定路径是否存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean checkExist(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            return false;
        }
        return true;
    }

    /**
     * 获取指定路径下的数据
     *
     * @param path
     * @return
     * @throws Exception
     */
    public byte[] getData(String path) throws Exception {
        return client.getData().forPath(path);
    }

    /**
     * 设置指定路径数据，需确保该路径已被创建
     *
     * @param path
     * @param data
     * @throws Exception
     */
    public void setData(String path, byte[] data) throws Exception {
        client.setData().forPath(path, data);
    }

    /**
     * 强制保证删除指定节点
     *
     * @param path
     * @throws Exception
     */
    public void delete(String path) throws Exception {
        //client.delete().forPath(path);
        //client.delete().guaranteed().forPath(path);
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
    }

    public void addlistener(String path) throws Exception {
        final NodeCache nodeCache = new NodeCache(client,path);
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
                LOG.info("nodeCache data : "+ data);
                if (data.equals("0")){
                    String[] cmds = { "/bin/sh", "-c", "systemctl stop service" };
                    LinuxUtil.execShell(cmds);
                    LOG.info("systemctl stop service");
                }

            }
        });
    }

    @Override
    protected void finalize() throws Throwable {
        if (client != null) {
            client.close();
        }
        super.finalize();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }


    public static void main(String[] args) {

    }
}
