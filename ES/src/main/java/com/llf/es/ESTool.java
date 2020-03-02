package com.llf.es;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ESTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ESTool.class);
    private static TransportClient transportClient;
    private static JestClient jestClient;

    public ESTool() {
        try {
            transportClient = ESConnectFactory.getTransportClientInstance();
            jestClient = ESConnectFactory.getJestClientInstance();
            LOGGER.info(transportClient.nodeName());
            LOGGER.info(jestClient.toString());
        } catch (IOException e) {
            LOGGER.error(e.getMessage(),e);
        }
    }

    public SearchResult search(String[] indexName, String query) throws Exception {
        List<String> list = Arrays.asList(indexName);
        Search search = new Search.Builder(query)
                .addIndex(list)
                .build();
        return jestClient.execute(search);
    }

}
