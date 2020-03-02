package executor.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import java.io.IOException;

public class CuratorUtils {

    public static CuratorFramework getCuratorClient() throws IOException {
        String address = "10.33.57.46:2181,10.33.57.47:2181,10.33.57.48:2181";
        System.out.println("create curator client:"+address);
        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000,3,5000);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(address).sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000).retryPolicy(exponentialBackoffRetry).build();
        client.start();
        return client;
        //return CuratorFrameworkFactory.newClient(address, new ExponentialBackoffRetry(1000, 3));

    }
}
