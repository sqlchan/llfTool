package zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class ZKUtils implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZKUtils.class);
    private static final byte[] LOCK = {};
    private volatile static ZKUtils zkUtilsInstance = null;
    private CuratorFramework client;

    private static final int ZK_CONNECTION_TIMEOUT_MILLIS = 1;
    // zk超时时间
    private static final int ZK_SESSION_TIMEOUT_MILLIS = 1;
    private static final int RETRY_WAIT_MILLIS = 1;
    // 出错尝试次数
    private static final int MAX_RECONNECT_ATTEMPTS = 1;

    private ZKUtils() {
        /*
         * curator实现zookeeper锁服务
         */
        try {
            String zkHost = "1.1.1.1";
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS);
            client = CuratorFrameworkFactory.builder().connectString(zkHost).retryPolicy(retryPolicy)
                    .sessionTimeoutMs(ZK_SESSION_TIMEOUT_MILLIS)
                    .connectionTimeoutMs(ZK_CONNECTION_TIMEOUT_MILLIS).build();

            client.start();
        } catch (Exception e) {
            LOG.error("Init ZKUtils failed, for ", e);
        }
    }

    public CuratorFramework getClient() {
        return client;
    }

    public boolean checkExist(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    public static ZKUtils getInstance() {
        if (zkUtilsInstance == null) {
            synchronized (LOCK) {
                if (zkUtilsInstance == null) {
                    zkUtilsInstance = new ZKUtils();
                }
            }
        }
        return zkUtilsInstance;
    }
}

