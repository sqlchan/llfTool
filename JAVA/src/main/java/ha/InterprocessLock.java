package ha;

import executor.discovery.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class InterprocessLock {
    static CountDownLatch countDownLatch = new CountDownLatch(10);

    public static void main(String[] args) throws IOException {
        CuratorFramework zkclient = CuratorUtils.getCuratorClient();
        String lock_path = "/lock_path";
        InterProcessMutex lock = new InterProcessMutex(zkclient,lock_path);
        for (int i=0;i<100;i++){
            new Thread(new TestThread(i,lock)).start();
        }
    }
    static class TestThread implements Runnable{
        private Integer threadFlag;
        private InterProcessMutex lock;

        public TestThread(Integer threadFlag, InterProcessMutex lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.acquire();
                System.out.println("thread "+threadFlag+" get thread");
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
