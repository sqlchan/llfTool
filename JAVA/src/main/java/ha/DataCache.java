package ha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang.StringUtils;

public class DataCache {

    private static final Logger LOG = LoggerFactory.getLogger(DataCache.class);

    //task任务数限制
    private static Cache<String, Integer> taskCache;

    //配置项缓存
    private static Cache<String, Object> confCache;

    //服务状态缓存,0表示正常,-1表示异常
    private static Cache<String, Integer> statusCache;

    //hrms注册信息缓存
    private static Cache<String, Object> hrmsCache;

    // 并发控制
    private static byte[] taskLocal = {};  //任务数限制
    private static byte[] confLocal = {};  //配置项限制
    private static byte[] statusLocal = {};  //配置项限制
    private static byte[] hrmsLocal = {};

    /**
     * 初始化
     */
    static {
        taskCache = CacheBuilder.newBuilder().build();
        confCache = CacheBuilder.newBuilder().build();
        statusCache = CacheBuilder.newBuilder().build();
        hrmsCache = CacheBuilder.newBuilder().build();
    }

    /**
     * 获取对应key的配置项
     *
     * @param key
     * @return
     */
    public static Object getConfCache(String key) {

        if (null == confCache.getIfPresent(key)) {
            return 0;
        }

        return confCache.getIfPresent(key);
    }

    /**
     * 更新对应key的配置项
     *
     * @param key
     * @param value
     * @return
     */
    public static boolean updateConfCache(String key, Object value) {
        if (StringUtils.isBlank(key) || null == value) {
            return false;
        }

        synchronized (confLocal) {
            confCache.put(key, value);
        }

        return true;
    }

    /**
     * 获取对应key的配置项
     *
     * @param key
     * @return
     */
    public static Integer getStatusCache(String key) {

        if (null == statusCache.getIfPresent(key)) {
            return null;
        }

        return statusCache.getIfPresent(key);
    }

    /**
     * 更新对应key的配置项
     *
     * @param key
     * @param value
     * @return
     */
    public static boolean updateStatusCache(String key, Integer value) {
        if (StringUtils.isBlank(key) || null == value) {
            return false;
        }

        synchronized (statusLocal) {
            statusCache.put(key, value);
        }

        return true;
    }

    /**
     * 获取对应key的任务数
     *
     * @param key
     * @return
     */
    public static int getTaskNumCache(String key) {

        if (null == taskCache.getIfPresent(key)) {
            return 0;
        }
        return taskCache.getIfPresent(key);
    }

    /**
     * 前端提交任务数缓存修改
     *
     * @param type 1表示加 2表示减
     * @param key
     */
    public static boolean updateTaskNumCache(int type, String key) {
        if (type < 0 || StringUtils.isBlank(key)) {
            return false;
        }

        synchronized (taskLocal) {
            int value = getTaskNumCache(key);
            int newValue = value;

            if (type == 1) {   //加
                newValue++;
            } else if (value > 0 && type == 2) {   //减
                newValue--;
            } else {
                return false;  //操作非法
            }

            taskCache.put(key, newValue);
        }

        return true;

    }

    public static Object getHrmsCache(String key) {
        return hrmsCache.getIfPresent(key);
    }

    public static <T> Boolean updateHrmsCache(String key, T value) {
        if (StringUtils.isBlank(key)) {
            return false;
        }
        synchronized(hrmsLocal) {
            hrmsCache.put(key, value);
        }
        return true;
    }

    public static Boolean removeHrmsCache(String key) {
        try {
            hrmsCache.invalidate(key);
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            return false;
        }
        return true;
    }
}

