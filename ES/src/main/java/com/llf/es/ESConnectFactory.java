package com.llf.es;

import com.google.gson.GsonBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ESConnectFactory {

    private static Logger LOG = LoggerFactory.getLogger(ESConnectFactory.class);

    private static volatile TransportClient transportClient;
    //并发加锁对象；开销最小；
    //private static final byte[] LOCK = new byte[0];
    private static volatile JestClient jestClient;
    private ESConnectFactory() { }


    public static JestClient getJestClient(){
        JestClientFactory factory = new JestClientFactory();
        String addrs = "10.33.57.47:9200";
        factory.setHttpClientConfig(httpClientConfig(addrs));
        LOG.info("Jest client create!");
        return factory.getObject();
    }

    public synchronized static JestClient getJestClientInstance() {
        if (jestClient == null) {
            jestClient = getJestClient();
        }
        return jestClient;
    }

    private static HttpClientConfig httpClientConfig(String addrs){
        String [] urls = addrs.split(",");
        List<String> nodeList = new ArrayList<>();
        for (int i = 0; i < urls.length; i++){
            nodeList.add("http://" + urls[i]);
        }
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://10.33.57.47:9200")                .multiThreaded(true)
                .defaultMaxTotalConnectionPerRoute(2)
                .maxTotalConnection(10)
                .build();
        return httpClientConfig;
    }

    public static TransportClient getTransportClientInstance() throws IOException {
        if (transportClient == null) {
            synchronized (ESConnectFactory.class) {
                if (transportClient == null) {
                    transportClient = getTransportClient();
                }
            }
        }
        return transportClient;
    }

    private static TransportClient getTransportClient() throws IOException {
        Settings settings = Settings.builder()
                .put("cluster.name", "SERVICE-ELASTICSEARCH-retro")
                .put("client.transport.sniff", true)
                .build();

        TransportClient transportClient = new PreBuiltTransportClient(settings);
        String[] ips = "10.19.154.119".split(",");
        for (String ip : ips) {
            try {
                InetSocketTransportAddress ist = new InetSocketTransportAddress(InetAddress.getByName(ip), 9300);
               transportClient.addTransportAddress(ist);
            }catch (Exception e){
                LOG.error(e.getMessage(),e);
            }
        }
        return transportClient;
    }

}

