package disruptor.demo3;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import disruptor.demo1.TradeTransaction;
import disruptor.demo1.TradeTransactionInDBHandler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * EventHandlerGroup
 * 一组 EventProcessor ，用作 Disruptor 的一部分。
 *      then: 设置批处理程序以使用环形缓冲区中的事件。 这些处理程序仅在该组中的每个EventProcessor处理事件之后才处理事件。
 *      handleEventsWith： A必须在B前执行  dw.after(A).handleEventsWith(B);
 *      handleEventsWithWorkerPool： A必须在BC工作池之前  dw.after(A).handleEventsWithWorkerPool(B, C);
 */

public class Demo3 {
    public static void main(String[] args) throws InterruptedException {
        long beginTime=System.currentTimeMillis();

        int bufferSize=1024;
        ExecutorService executor=Executors.newFixedThreadPool(4);
        //这个构造函数参数，相信你在了解上面2个demo之后就看下就明白了，不解释了~
        Disruptor<TradeTransaction> disruptor= new Disruptor<>(() -> new TradeTransaction(), bufferSize, executor, ProducerType.SINGLE, new BusySpinWaitStrategy());

        // handleEventsWith: 设置事件处理程序来处理来自循环缓冲区的事件。这些处理程序将在事件可用时立即并行地处理它们。
        //使用disruptor创建消费者组C1,C2
        EventHandlerGroup<TradeTransaction> handlerGroup=disruptor.handleEventsWith(new TradeTransactionVasConsumer(),new TradeTransactionInDBHandler());

        TradeTransactionJMSNotifyHandler jmsConsumer=new TradeTransactionJMSNotifyHandler();
        //声明在C1,C2完事之后执行JMS消息发送操作 也就是流程走到C3
        handlerGroup.then(jmsConsumer);


        disruptor.start();//启动
        CountDownLatch latch=new CountDownLatch(1);
        //生产者准备
        executor.submit(new TradeTransactionPublisher(latch, disruptor));
        latch.await();//等待生产者完事.
        disruptor.shutdown();
        executor.shutdown();

        System.out.println("总耗时:"+(System.currentTimeMillis()-beginTime));
    }
}

