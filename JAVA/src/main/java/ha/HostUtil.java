package ha;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HostUtil.class);

    /**
     * 获取本机主机名
     *
     * @return
     */
    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("获取主机名出错", e);
            throw new RuntimeException(e);
        }
    }
}
