package com.llf.kafka.batch.consumer;

import com.llf.kafka.batch.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class KafkaBatchConsumer<T> extends AbstractConsumer<T> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaBatchConsumer.class);
    private int batchsize =100;
    private long submitTimeOut =100;
    private boolean isClosed = false;

    private long times4SubmitTask = System.currentTimeMillis();
    private List<T> queue = new ArrayList<>();
    private byte[] lock = {};
    // 超时判断线程池
    protected ExecutorService executor = Executors.newSingleThreadExecutor();
    private boolean submited = false;

    public KafkaBatchConsumer(Configuration configuration, String topic) {
        super(configuration, topic);
        configuration.addProperty("auto.commit.enable","false");
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (!isClosed){
                    if (queue.size() > 0 && submitTimeOut < (System.currentTimeMillis() - times4SubmitTask) && !submited){
                        synchronized (lock){
                            if (queue.size() > 0 ){
                                //LOGGER.info("Timeout.Start batch handle.");
                                // 当提交时间达到超时时间，且队列中仍有记录，提交任务
                                submit();
                            }
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void submit(){
        if (queue.size() == 0) logger.error("none");
        submited = true;
        try {
            batchHandle(queue);
            queue.clear();
            getConsumer().commitOffsets();
        }finally {
            submited = false;
        }

    }

    public abstract void batchHandle(List<T> list);

    @Override
    public List<T> deserialize(byte[] msg) {
        return null;
    }

    @Override
    public void handle(List<T> t) throws IOException {
        synchronized (lock){
            if (queue.size() == 0 ) times4SubmitTask = System.currentTimeMillis();
            if (null != t && t.size() > 0) queue.addAll(t);
            if (batchsize < queue.size() && !submited) submit();
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (null != executor) executor.shutdownNow();
        isClosed = true;
    }
}
