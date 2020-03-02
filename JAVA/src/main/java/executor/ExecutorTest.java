package executor;

import javax.swing.text.StyledEditorKit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ExecutorTest {
    private static boolean istrue = true;
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.execute(new Runnable() {
            int i =0;
            @Override
            public void run() {
                while (istrue){
                    i=i + 1;
                    System.out.println("newSingleThreadExecutor: "+i);
                    if (i> 10) istrue = false;
                }
            }
        });
        //future.get();
        //executor.shutdown();
    }
}
