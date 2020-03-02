package disruptor.demo2;

import com.lmax.disruptor.*;
import disruptor.demo1.TradeTransaction;
import disruptor.demo1.TradeTransactionInDBHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * WorkerPool
 * WorkerPool包含一个 WorkProcessor 池，这些池将消耗序列，因此可以在一组worker池中处理作业。
 * 每个 WorkProcessor 都管理并调用 WorkHandler 来处理事件。
 *  start => executor.execute(processor);  通知工作人员池按顺序处理事件。
 */
// 使用workerpool辅助创建消费者
public class Demo2 {
    public static void main(String[] args) throws InterruptedException {
        int BUFFER_SIZE=1024;
        int THREAD_NUMBERS=4;
        EventFactory<TradeTransaction> eventFactory= () -> new TradeTransaction();
        RingBuffer<TradeTransaction> ringBuffer=RingBuffer.createSingleProducer(eventFactory, BUFFER_SIZE);
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUMBERS);

        WorkHandler<TradeTransaction> workHandlers=new TradeTransactionInDBHandler();

        WorkerPool<TradeTransaction> workerPool=new WorkerPool<TradeTransaction>(ringBuffer, sequenceBarrier, new IgnoreExceptionHandler(), workHandlers);

        workerPool.start(executor);

        //下面这个生产8个数据，图简单就写到主线程算了
        for(int i=0;i<8;i++){
            long seq=ringBuffer.next();
            ringBuffer.get(seq).setPrice(Math.random()*9999);
            ringBuffer.publish(seq);
        }

        Thread.sleep(1000);
        workerPool.halt();
        executor.shutdown();
    }
}

