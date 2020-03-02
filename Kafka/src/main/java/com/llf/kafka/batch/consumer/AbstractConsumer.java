package com.llf.kafka.batch.consumer;

import com.llf.kafka.batch.util.Configuration;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class AbstractConsumer<T> implements IConsumer{
    private static final Logger logger = LoggerFactory.getLogger(AbstractConsumer.class);

    public String _topic;
    // 消息接收标志
    private boolean acceptFlag = true;
    private ConsumerConnector _consumer;
    protected Configuration _configuration;
    private CountDownLatch latch;

    public AbstractConsumer(Configuration configuration , String topic){
        this._topic = topic;
        if (null != configuration){
            this._configuration = new Configuration((Properties) configuration.getConfig().clone());
        }
    }

    public ConsumerConnector getConsumer(){
        while (null == _consumer){
            _consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(_configuration.getConfig()));
        }
        return _consumer;
    }

    public abstract List<T> deserialize(byte[] msg);
    public abstract void handle(List<T> t) throws IOException;

    @Override
    public void run() {
        Map<String ,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(_topic,Integer.valueOf(1));
        Map<String , List<KafkaStream<byte[],byte[]>>> consumerMap = getConsumer().createMessageStreams(topicCountMap);

        for(KafkaStream<byte[], byte[]> stream : consumerMap.get(_topic)){
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (accept() && it.hasNext()){
                MessageAndMetadata<byte[], byte[]> next = it.next();
                byte[] dstMsg = next.message();
                List<T> list = deserialize(dstMsg);
                try {
                    handle(list);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private boolean accept(){
        while (!acceptFlag){
            logger.info("acceptFlag1: " + acceptFlag);
            try {
                latch = new CountDownLatch(1);
                boolean await = latch.await(100,TimeUnit.SECONDS);
                if (await) latch = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return accept();
            }
            logger.info("acceptFlag2: " + acceptFlag);
        }
        return acceptFlag;
    }

    public void setAcceptFlag(boolean acceptFlag) {
        this.acceptFlag = acceptFlag;
    }

    @Override
    public void close() throws IOException {
        if (null != _consumer) _consumer.shutdown();
    }
}
