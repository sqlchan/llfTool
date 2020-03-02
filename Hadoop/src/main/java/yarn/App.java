package yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.*;

public class App {
    public static void main(String[] args) throws IOException, YarnException {
        Configuration conf = new Configuration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
        YarnApplicationState applicationStates = YarnApplicationState.FINISHED;
        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
        appStates.add(applicationStates);
        List<ApplicationReport> applicationReportList = client.getApplications(appStates);


        List<String> applist = new ArrayList<>();
        List<String> statelist = new ArrayList<>();
        for (ApplicationReport application : applicationReportList){
            applist.add(application.getName());
            statelist.add(String.valueOf(application.getYarnApplicationState()));
        }
        for (int i=0;i<applist.size()-1;i++){
            System.out.println("app: "+applist.get(i)+", state: "+statelist.get(i));
        }
        if (applist.contains("WiFiStreaming")){
            System.out.println("ok");
        }

    }
}
