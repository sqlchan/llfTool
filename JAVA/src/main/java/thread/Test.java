package thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static ExecutorService executor = Executors.newFixedThreadPool(1);
    public static void main(String[] args) {

        executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("haha");
            }
        });
    }
}
