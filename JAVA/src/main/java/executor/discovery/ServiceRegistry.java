package executor.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.Collection;

public class ServiceRegistry {
    private ServiceDiscovery<ServicePayLoad> serviceDiscovery;
    private  CuratorFramework client ;
    public ServiceRegistry(CuratorFramework client,String basePaht){
        this.client = client;
        serviceDiscovery = ServiceDiscoveryBuilder.builder(ServicePayLoad.class)
                .client(client).serializer(new JsonInstanceSerializer<>(ServicePayLoad.class))
                .basePath(basePaht).build();

    }
    public void updateService(ServiceInstance<ServicePayLoad> instance) throws Exception {
        serviceDiscovery.updateService(instance);
    }
    public void registerService(ServiceInstance<ServicePayLoad> instance) throws Exception {
        serviceDiscovery.registerService(instance);
    }

    public void unregisterService(ServiceInstance<ServicePayLoad> instance) throws Exception {
        serviceDiscovery.unregisterService(instance);
    }

    public Collection<ServiceInstance<ServicePayLoad>> queryForInstances(String name) throws Exception {
        return serviceDiscovery.queryForInstances(name);
    }

    public ServiceInstance<ServicePayLoad> queryForInstance(String name, String id) throws Exception {
        return serviceDiscovery.queryForInstance(name, id);
    }

    public void start() throws Exception {
        serviceDiscovery.start();
    }

    public void close() throws Exception {
        serviceDiscovery.close();
    }
}
