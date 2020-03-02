package com.llf.kafka.batch.util;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

public class Configuration implements Serializable {
    private static final long serialVersionUID = 1L;

    public final static String PRODUCER_TYPE = "producer.type";
    public final static String SERIALIZER_CLASS = "serializer.class";
    public final static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public final static String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public final static String GROUP_ID = "group.id";
    public final static String ZOOKEEPER_SYNC_TIME_MS = "zookeeper.sync.time.ms";
    public final static String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    public final static String AUTO_COMMIT_ENABLE = "auto.commit.enable";
    public final static String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
    public final static String FETCH_MESSAGE_MAX_BYTES = "fetch.message.max.bytes";
    public final static String BATCH_SIZE = "batch.size";
    public final static String SUBMIT_TIMEOUT = "submit.timeout";
    public final static String AUTO_OFFSET_RESET = "auto.offset.reset";
    public final static String REQUEST_REQUIRED_ACKS = "request.required.acks";

    public static final String MAX_REQUEST_SIZE = "max.request.size";
    public static final String COSUMER_KEY = "cosumer.key";

    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_NAME = "hbase.zookeeper.property.clientPort";
    public static final String HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD_NAME = "hbase.client.scanner.timeout.period";
    public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
    private Properties _properties;

    public Configuration(){_properties = new Properties();}

    public Configuration(Map<String, String> properties){
        _properties = new Properties();
        for ( Map.Entry<String, String> entry : properties.entrySet()){
            _properties.put(entry.getKey(),entry.getValue());
        }
    }

    public Configuration(Properties properties){_properties = properties;}

    public void addProperty(String key, String value) {
        _properties.put(key, value);
    }

    public Properties getConfig() {
        return _properties;
    }

    public String getProperty(String key) {
        return (_properties.get(key) != null) ? (String) _properties.get(key) : null;
    }
}
