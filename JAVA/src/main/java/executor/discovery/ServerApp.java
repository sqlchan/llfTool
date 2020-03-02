package executor.discovery;

import com.alibaba.fastjson.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class ServerApp {

    public static final String BASE_PATH = "services";
    public static final String SERVICE_NAME = "com.bytebeats.service.HelloService";

    public static void main(String[] args) {

        CuratorFramework client = null;
        ServiceRegistry serviceRegistry = null;
        try{
            client = CuratorUtils.getCuratorClient();
            client.start();

            serviceRegistry = new ServiceRegistry(client, BASE_PATH);
            serviceRegistry.start();

            //注册两个service 实例
            ServiceInstance<ServicePayLoad> host1 = ServiceInstance.<ServicePayLoad>builder()
                    .id("host1")
                    .name(SERVICE_NAME)
                    .port(21888)
                    .address("10.99.10.1")
                    .payload(new ServicePayLoad("HZ", 5))
                    .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                    .build();

            serviceRegistry.registerService(host1);

            ServiceInstance<ServicePayLoad> host2 = ServiceInstance.<ServicePayLoad>builder()
                    .id("host2")
                    .name(SERVICE_NAME)
                    .port(21888)
                    .address("10.99.1.100")
                    .payload(new ServicePayLoad("QD", 3))
                    .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                    .build();

            serviceRegistry.registerService(host2);

            System.out.println("register service success...");

            TimeUnit.MINUTES.sleep(2);

            Collection<ServiceInstance<ServicePayLoad>> list = serviceRegistry.queryForInstances(SERVICE_NAME);
            if(list!=null && list.size()>0){
                System.out.println("service:"+SERVICE_NAME+" provider list:"+ JSONObject.toJSONString(list));
            } else {
                System.out.println("service:"+SERVICE_NAME+" provider is empty...");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(serviceRegistry!=null){
                try {
                    serviceRegistry.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            client.close();
        }

    }
}

