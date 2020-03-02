package disruptor.demo3;

import com.lmax.disruptor.dsl.Disruptor;
import disruptor.demo1.TradeTransaction;
import java.util.concurrent.CountDownLatch;

/**
 * Disruptor
 * 用于设置屏障，保证事件顺序
 *      publishEvent: 将事件发布到循环缓冲区
 *      start: 启动事件处理器并返回完全配置的环形缓冲区，在添加了所有事件处理器之后，只能调用此方法一次
 *      get: 获取RingBuffer中给定序列的事件
 *      handleEventsWith: 设置事件处理程序来处理来自循环缓冲区的事件。这些处理程序将在事件可用时立即并行地处理它们。
 */

public class TradeTransactionPublisher implements Runnable{
    Disruptor<TradeTransaction> disruptor;
    private CountDownLatch latch;
    private static int LOOP=10;//模拟一千万次交易的发生

    public TradeTransactionPublisher(CountDownLatch latch,Disruptor<TradeTransaction> disruptor) {
        this.disruptor=disruptor;
        this.latch=latch;
    }

    @Override
    public void run() {
        TradeTransactionEventTranslator tradeTransloator=new TradeTransactionEventTranslator();
        for(int i=0;i<LOOP;i++){
            disruptor.publishEvent(tradeTransloator);
        }
        latch.countDown();
    }

}

