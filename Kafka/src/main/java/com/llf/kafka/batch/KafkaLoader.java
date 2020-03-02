package com.llf.kafka.batch;

import com.llf.kafka.batch.consumer.KafkaBatchConsumer;
import com.llf.kafka.batch.util.Configuration;
import com.llf.kafka.batch.util.IConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaLoader implements Closeable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaLoader.class);

    private List<String> cache;
    private IConverter converter;
    private byte[] lock = {};
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private transient KafkaBatchConsumer consumer;

    public KafkaLoader(){
        cache = new ArrayList<>();
        converter = new IConverter() {
            @Override
            public List<String> byte2ListExcludeNonDay(byte[] msg) {
                return null;
            }
        };

        Properties props = new Properties();
        Configuration configuration = new Configuration(props);
        consumer = new KafkaBatchConsumer<String>(configuration,"") {
            @Override
            public void batchHandle(List<String> list) {
                synchronized (lock){
                    if (null != list && list.size() > 0){
                        cache.addAll(list);
                        if (cache.size() > 5000000 )this.setAcceptFlag(false);
                        if (cache.size() == 0)this.setAcceptFlag(true);
                    }
                }
            }
        };

        executor.submit(consumer);
    }

    public List<String> load(){
        List<String> result;
        synchronized (lock){
            result = cache;
            cache = new ArrayList<>();
            consumer.setAcceptFlag(true);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (null != consumer) consumer.close();
        if (null != executor) executor.shutdown();
    }
}
