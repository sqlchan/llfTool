//package ha;
//
//public class WiFiContext {
//    private boolean leaderLatchInitedInd = false;//主节点竞争客户端初始化标识
//    public void initLeaderLatch() {
//        if (!leaderLatchInitedInd) {
//            //获取本机主机名
//            final String hostName = HostUtil.getHostName();
//            //获取端口
//            int port = 8888;
//
//            final LeaderLatchClient.NodeInfo nodeInfo = new LeaderLatchClient.NodeInfo(hostName, port);
//            leaderLatchClient = new LeaderLatchClient(ZKUtil.getInstence().getClient(), nodeInfo, WiFiConstants
//                    .WIFI.WIFI_LEADER_LATCH_PATH, new LeaderLatchListener() {
//
//                @Override
//                public void isLeader() {
//                    /**
//                     * 1. 数据同步以及加载
//                     */
//                    LOGGER.info("{} is leader ***************", hostName);
//
//                    try {
//                        Thread.sleep(waitTimeMillis);
//                    } catch (InterruptedException e) {
//                        LOGGER.warn("WBP: Wait {} before init spark context...", waitTimeMillis);
//                    }
//                    /**
//                     * master 节点判断
//                     */
//                    if (WiFiConstants.WIFI.WIFI_DEPLOY_TYPE_CLUSTER.equals(DataCache.getConfCache(WiFiConstants.WIFI
//                            .WIFI_DEPLOY_TYPE_NAME)) && WiFiContext._INSTANCE.getLeaderLatchClient().isLeader()) {
//                        try {
//                            LOGGER.info("kill spark application start...");
//                            //初始化前kill spark application
//                            ISparkRunMode sparkRunModeImpl = (ISparkRunMode) AppContext.getBean(WiFiConstants.COMMON.SPRING_BEAN_SPARKRUNMODEIMPL);
//                            sparkRunModeImpl.killApplication();
//                        } catch (Exception e) {
//                            LOGGER.error("error:{}",e);
//                        } finally {
//                            LOGGER.info("kill spark application end...");
//                        }
//
//                    }
//                    //初始化wifi应用
//                    initWiFi();
//                }
//
//                @Override
//                public void notLeader() {
//                    LOGGER.info("{} is not leader,close manager,close sparkContext**********", hostName);
//                    DataLoader.setIsLoaderFinished(true);
//                    /**
//                     * 执行器 关闭
//                     */
//                    for (Manager manager : DataManagerBuilder.getManagerCache().values()) {
//                        manager.close();
//                    }
//
//                    /**
//                     * sc 关闭
//                     */
//                    if (WiFiSparkContext.getContext() != null) {
//                        try{
//                            WiFiSparkContext.getContext().stop();
//                        }catch (Exception e){
//                            LOGGER.error("close sc error ..");
//                        }
//                    }
//                    /**
//                     * 关闭线程池
//                     */
//                    try{
//                        if(MacInfoLoadTask.getDataLoaderThreadExecutor()!=null&& !MacInfoLoadTask.getDataLoaderThreadExecutor().isShutdown()){
//                            MacInfoLoadTask.dataLoaderThreadExecutor.shutdownNow();
//                        }
//                        if(MacInfoLoadTask.getIncrementLoaderThreadExecutor()!=null&& !MacInfoLoadTask.getIncrementLoaderThreadExecutor().isShutdown()){
//                            MacInfoLoadTask.incrementLoaderThreadExecutor.shutdownNow();
//                        }
//                    }catch (Exception e){
//                        LOGGER.error("close thread pool error ..");
//                    }
//                    /**
//                     * 关闭后台线程服务
//                     */
//                    try{
//                        if(RddMergeService.getRunnable()!=null){
//                            RddMergeService.getRunnable().stop();
//                        }
//                        if(JobTimeOutService.getRunnable()!=null){
//                            JobTimeOutService.getRunnable().stop();
//                        }
//                    }catch (Exception e){
//                        LOGGER.error("close group thread  error ..");
//                    }
//
//                }
//            });
//
//            try {
//                leaderLatchClient.start();
//            } catch (Exception e) {
//                String errMsg = "Leader latch client start error.";
//                LOGGER.error(errMsg);
//                throw new RuntimeException(errMsg);
//            }
//
//            leaderLatchInitedInd = true;
//        } else {
//            LOGGER.warn("Leader latch client has init.");
//        }
//    }
//}
