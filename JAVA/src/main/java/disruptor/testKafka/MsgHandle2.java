package disruptor.testKafka;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import disruptor.demo1.TradeTransaction;

public class MsgHandle2 implements EventTranslator<Msg> {
    private String msg;

    public MsgHandle2(String msg) {
        this.msg = msg;
    }


    @Override
    public void translateTo(Msg event, long sequence) {
        this.generateTradeTransaction(event);
    }
    private Msg generateTradeTransaction(Msg trade){
        trade.setMessage(msg);
        return trade;
    }
}