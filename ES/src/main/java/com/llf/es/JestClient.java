package com.llf.es;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

public class JestClient {
    public io.searchbox.client.JestClient jestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://10.19.154.119:9200")
                        .multiThreaded(true)
                        .defaultMaxTotalConnectionPerRoute(2)
                        .maxTotalConnection(10)
                        .build());
        return factory.getObject();
    }

    public static void main(String[] args) {
        JestClient test = new JestClient();
        io.searchbox.client.JestClient jestClient = test.jestClient();
        System.out.println(jestClient);
    }
}
