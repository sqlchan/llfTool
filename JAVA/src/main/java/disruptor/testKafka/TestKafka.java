package disruptor.testKafka;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import disruptor.demo1.TradeTransaction;
import disruptor.demo1.TradeTransactionInDBHandler;
import disruptor.demo3.TradeTransactionJMSNotifyHandler;
import disruptor.demo3.TradeTransactionVasConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestKafka {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executor=Executors.newFixedThreadPool(4);
        Disruptor<Msg> disruptor= new Disruptor<>(() -> new Msg(), 1024, executor, ProducerType.SINGLE, new BusySpinWaitStrategy());
        EventHandlerGroup<Msg> handlerGroup=disruptor.handleEventsWith(new MsgHandle());
        MsgHandle1 handle1=new MsgHandle1();
        handlerGroup.then(handle1);
        disruptor.start();

        executor.submit(()->{
            Properties props = new Properties();
            props.put("bootstrap.servers", "10.33.57.46:9092");
            props.put("group.id", "sendmac");// 不同ID 可以同时订阅消息
            props.put("auto.offset.reset","earliest");//latest, earliest, none
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("MAC_INFO_TOPIC"));
            long seq;
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord consumerRecord : records){
                    String s = (String) consumerRecord.value();
                    System.out.println(s);
                    Msg msg = new Msg();
                    msg.setMessage(s);

                }
            }

        });

        Thread.sleep(500000);



    }
}
