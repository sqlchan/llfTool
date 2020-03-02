package com.llf.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SendData implements Closeable {
    private static Logger logger = LoggerFactory.getLogger(SendData.class);
    private volatile Thread shutdownHook;
    private volatile int threadNum = 1;
    private ExecutorService executor = Executors.newFixedThreadPool(threadNum);

    public static void main(String[] args) {
        SendData mainClient = new SendData();
        System.out.println("启动成功!");
        mainClient.start();

        mainClient.close();
    }

    public void send(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.33.57.46:9092");
        props.put("group.id", "sendmac");// 不同ID 可以同时订阅消息
        props.put("zookeeper.sync.timeout.ms", "2000");
        props.put("auto.commit.enable", "true");
        props.put("zookeeper.session.timeout.ms", "60000");
        props.put("auto.offset.reset","earliest");//latest, earliest, none
        props.put("fetch.message.max.bytes", "" + (8l * 1024 * 1024));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("hik_mac_info"));

        Properties properties = new Properties();
        properties.put("bootstrap.servers","10.19.154.119:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<byte[],byte[]> producer = new KafkaProducer<>(properties);


        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord consumerRecord : records){
                    System.out.println(new String((byte[]) consumerRecord.value()));
//                    byte[] bytes1 = JSONObject.toJSONString(s).getBytes();
//                    byte[] bytes2 = new byte[bytes1.length+2];
//                    bytes2[0] = 1;
//                    bytes2[1] = 0;
//                    System.arraycopy(bytes1, 0, bytes2, 2, bytes1.length);
                    producer.send(new ProducerRecord<byte[],byte[]>("hik_mac_info", (byte[]) consumerRecord.value()));
                }
                producer.flush();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
            producer.close();
        }
    }

    public void start(){
        shutdownHook = new Thread() {
            @Override
            public void run() {
                executor.shutdown();
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        send();
    }


    @Override
    public synchronized void close() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ex) {
                //ignore shutting down status
            }
            shutdownHook.start();
            shutdownHook = null;
        }
    }
}
