package disruptor.demo3;

import com.lmax.disruptor.EventHandler;
import disruptor.demo1.TradeTransaction;

public class TradeTransactionJMSNotifyHandler implements EventHandler<TradeTransaction> {

    @Override
    public void onEvent(TradeTransaction event, long sequence,
                        boolean endOfBatch) throws Exception {
        //do send jms message
    }
}

