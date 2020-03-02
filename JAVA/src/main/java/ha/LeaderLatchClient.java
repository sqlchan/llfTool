package ha;

import com.alibaba.fastjson.JSON;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public class LeaderLatchClient implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderLatchClient.class);

    private byte[] lock = new byte[0];

    //private static Executor executor = Executors.newCachedThreadPool();
    private static final String LATCH_PATH_PARENT = "/wbp_leader_latch";//默认
    private static final String LEADER_INFO_PATH = "/leader_info";//节点信息路径

    private final LeaderLatch leaderLatch;
    private NodeInfo leaderInfo;//主节点信息
    private final NodeInfo localInfo;//本地节点信息
    private final CuratorFramework client;//zookeeper连接客户端
    private NodeCache nodeCache;//节点缓存
    private final String leaderInfoPath;//主节点信息缓存路径
    private String latchPath;//主节点竞争路径
    private boolean cacheLeaderInd = false;//是否开启缓存主节点信息功能，默认不开启

    public LeaderLatchClient(CuratorFramework client, NodeInfo localInfo, String latchNode, LeaderLatchListener...
            latchListeners) {
        String errMsg;
        if (client == null || !client.getState().equals(CuratorFrameworkState.STARTED)) {
            errMsg = "Zookeeper Client is null or not start.";
            LOGGER.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        latchPath = genLeaderLatchPath(latchNode);

        if (localInfo == null) {
            LOGGER.warn("Local node info is null,cache leader is off.");
            this.localInfo = null;
            this.leaderInfoPath = null;
        } else {
            LOGGER.info("Local node info is not null,cache leader is on.");
            this.localInfo = localInfo;
            leaderInfoPath = genLeaderInfoPath(latchPath);
            cacheLeaderInd = true;
        }
        this.client = client;
        leaderLatch = new LeaderLatch(client, latchPath);

        for (LeaderLatchListener latchListener : latchListeners) {
            leaderLatch.addListener(latchListener);
        }

        //初始化主节点信息缓存
        initLeaderInfoCache();
    }

    /**
     * 开始竞争
     *
     * @throws Exception
     */
    public void start() throws Exception {
        leaderLatch.start();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        leaderLatch.close();
    }

    /**
     * 判断本节点是否为主节点
     *
     * @return
     */
    public boolean isLeader() {

        return leaderLatch.hasLeadership();
    }

    /**
     * 获取主节点信息
     *
     * @return
     */
    public NodeInfo getLeaderInfo() {
        if (cacheLeaderInd) {
            return leaderInfo;
        } else {
            String errMsg = "This client cache leader info is off can not get leader info.";
            LOGGER.error(errMsg);
            throw new UnsupportedOperationException(errMsg);
        }
    }

    /**
     * 初始化主节点信息缓存
     */
    private void initLeaderInfoCache() {
        if (cacheLeaderInd) {
            //添加主节点信息更新监听器
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    LOGGER.info("node [{}] now is leader node,update node info to zookeeper path [{}]", localInfo.toString
                            (), leaderInfoPath);
                    //创建主节点信息缓存路径
                    try {
                        if (client.checkExists().forPath(leaderInfoPath) == null) {
                            client.create().forPath(leaderInfoPath, JSON.toJSONString(localInfo).getBytes("UTF-8"));
                        } else {
                            client.setData().forPath(leaderInfoPath, JSON.toJSONString(localInfo).getBytes("UTF-8"));
                        }
                    } catch (Exception e) {
                        LOGGER.error("Update Leader info error.", e);
                    }
                }

                @Override
                public void notLeader() {
                }
            });

            //设置主节点信息缓存
            nodeCache = new NodeCache(client, leaderInfoPath);
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    String data = new String(nodeCache.getCurrentData().getData(), "UTF-8");
                    LOGGER.info("watch leader info change, now data is [{}]", data);
                    leaderInfo = JSON.parseObject(data, NodeInfo.class);
                }
            });

            try {
                nodeCache.start();
            } catch (Exception e) {
                LOGGER.error("Leader info change cache start error.");
            }
        }
    }

    /**
     * 生成主节点信息zk缓存路径
     *
     * @param latchPath
     * @return
     */
    private String genLeaderInfoPath(String latchPath) {
        if (latchPath.endsWith("/")) {
            latchPath = latchPath.substring(0, latchPath.length() - 1);
        }
        return latchPath + LEADER_INFO_PATH;
    }

    /**
     * 生成主节点竞争路径
     *
     * @param latchNode
     * @return
     */
    private String genLeaderLatchPath(String latchNode) {
        if (!latchNode.startsWith("/")) {
            latchNode = "/" + latchNode;
        }
        return LATCH_PATH_PARENT + latchNode;
    }

    /**
     * 主节点信息
     */
    public static class NodeInfo implements Serializable {
        private String nodeHost;
        private int nodePort;

        public NodeInfo() {
        }

        public NodeInfo(String nodeHost, int nodePort) {
            this.nodeHost = nodeHost;
            this.nodePort = nodePort;
        }

        public String getNodeHost() {
            return nodeHost;
        }

        public void setNodeHost(String nodeHost) {
            this.nodeHost = nodeHost;
        }

        public int getNodePort() {
            return nodePort;
        }

        public void setNodePort(int nodePort) {
            this.nodePort = nodePort;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}

