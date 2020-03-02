package disruptor.demo1;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;

import java.util.UUID;

/**
 * EventHandler
 * 当 RingBuffer 中 events 可用时，调用该回调接口  onEvent(T event, long sequence, boolean endOfBatch)
 * 当发布者已将事件发布到{ RingBuffer}时调用。
 * BatchEventProcessor}将分批读取来自{ RingBuffer}的消息，其中批处理是所有可处理的事件，而不必等待任何新事件到达。
 *
 * WorkHandler
 * 当工作单元在 RingBuffer中可用时 ，将为处理工作单元实现回调接口
 */
public class TradeTransactionInDBHandler implements EventHandler<TradeTransaction>,WorkHandler<TradeTransaction> {

    // 当发布者已将事件发布到{ RingBuffer}时调用。
    @Override
    public void onEvent(TradeTransaction event, long sequence,
                        boolean endOfBatch) throws Exception {
        System.out.println("============"+event.getPrice());
        this.onEvent(event);
    }

    // 表示需要处理的工作单元的回调。
    @Override
    public void onEvent(TradeTransaction event) throws Exception {
        //这里做具体的消费逻辑
        event.setId(UUID.randomUUID().toString());//简单生成下ID
        System.out.println(event.getId());
    }
}

