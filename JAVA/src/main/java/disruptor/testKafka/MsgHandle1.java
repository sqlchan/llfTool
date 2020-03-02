package disruptor.testKafka;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.EventHandler;

public class MsgHandle1 implements EventHandler<Msg> {

    @Override
    public void onEvent(Msg event, long sequence, boolean endOfBatch) throws Exception {
        System.out.println("<<<<<<  "+JSONObject.toJSONString(event));
    }
}