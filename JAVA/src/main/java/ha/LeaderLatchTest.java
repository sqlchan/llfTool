package ha;

import com.google.common.collect.Lists;
import executor.discovery.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.io.IOException;
import java.util.List;

public class LeaderLatchTest {
    static String path = "/lead_latch";

    public static void main(String[] args) throws Exception {
        List<CuratorFramework> clientlist = Lists.newArrayListWithCapacity(10);
        List<LeaderLatch> latchList = Lists.newArrayListWithCapacity(10);
        for (int i =0;i< 10 ;i++){
            CuratorFramework client = CuratorUtils.getCuratorClient();
            clientlist.add(client);
            LeaderLatch leaderLatch = new LeaderLatch(client,path,"client_"+i);
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    System.out.println("isleader : "+ leaderLatch.getId());
                }

                @Override
                public void notLeader() {
                    System.out.println("notLeader : "+ leaderLatch.getId());
                }
            });
            latchList.add(leaderLatch);
            leaderLatch.start();
            System.out.println(leaderLatch.getId()+ "  start");
        }
        checkLeader(latchList);
    }
    public static void checkLeader(List<LeaderLatch> leaderLatchList) throws InterruptedException, IOException {
        Thread.sleep(10000);
        for (int i=0;i<leaderLatchList.size();i++){
            LeaderLatch leaderLatch = leaderLatchList.get(i);
            if (leaderLatch.hasLeadership() ){
                System.out.println("leader is : "+leaderLatch.getId());
                leaderLatch.close();
                checkLeader(leaderLatchList);
            }
        }
    }
}
