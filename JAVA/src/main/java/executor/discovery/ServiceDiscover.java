package executor.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.strategies.RandomStrategy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceDiscover {
    private ServiceDiscovery<ServicePayLoad> serviceDiscover;
    private final ConcurrentHashMap<String,ServiceProvider<ServicePayLoad>> serviceProviderMap = new ConcurrentHashMap<>();
    public ServiceDiscover(CuratorFramework client, String basePath){
        serviceDiscover = ServiceDiscoveryBuilder.builder(ServicePayLoad.class)
                .client(client)
                .basePath(basePath)
                .serializer(new JsonInstanceSerializer<>(ServicePayLoad.class))
                .build();

    }
    public ServiceInstance<ServicePayLoad> getServiceProvider(String serviceName) throws Exception {
        ServiceProvider<ServicePayLoad> provider = serviceProviderMap.get(serviceName);
        if (null == provider){
            provider = serviceDiscover.serviceProviderBuilder().serviceName(serviceName)
                    .providerStrategy(new RandomStrategy<ServicePayLoad>()).build();
            ServiceProvider<ServicePayLoad> oldProvider = serviceProviderMap.putIfAbsent(serviceName,provider);
            if (null != oldProvider) provider = oldProvider;
            else provider.start();
        }
        return provider.getInstance();
    }
    public void start() throws Exception { serviceDiscover.start();}
    public void close() throws IOException {
        for (Map.Entry<String,ServiceProvider<ServicePayLoad>> me: serviceProviderMap.entrySet()){
            try {
                me.getValue().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        serviceDiscover.close();
    }


}
