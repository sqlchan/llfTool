package disruptor.demo3;

import com.lmax.disruptor.EventTranslator;
import disruptor.demo1.TradeTransaction;
import java.util.Random;

/**
 * EventTranslator
 * 声明事件，将数据转换为RingBuffer
 * 发布到RingBuffer时，请提供一个EventTranslator。 在发布序列更新之前，RingBuffer将按顺序选择下一个可用事件，并将其提供给EventTranslator（后者应更新事件）。
 *
 */

class TradeTransactionEventTranslator implements EventTranslator<TradeTransaction> {
    private Random random=new Random();
    @Override
    public void translateTo(TradeTransaction event, long sequence) {
        this.generateTradeTransaction(event);
    }
    private TradeTransaction generateTradeTransaction(TradeTransaction trade){
        trade.setPrice(random.nextDouble()*9999);
        return trade;
    }
}

