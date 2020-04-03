package presto;

import okhttp3.*;

import java.io.IOException;

public class okhttp {
    public static void get(){
        String url = "http://10.19.154.119:8481/bigdata/wbp/ISAPI/SDT/WIFI/Service/Statistics/queryCollectVirtualIdentityDistribution";
        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .build();
        Call call = okHttpClient.newCall(request);
        try {
            Response response = call.execute();
            System.out.println(response.body().string());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void post(){
        String url = "http://10.19.154.119:8481/bigdata/wbp/ISAPI/SDT/WIFI/Service/Search/generalQueryMacs";
        OkHttpClient okHttpClient = new OkHttpClient();

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, "{\"bigData\":{\"data\":{\"collectTime\":\"2020-02-21T05:13:20.000+08:00,2020-03-29T05:13:20.000+08:00\"},\"orderType\":\"desc\",\"orderField\":\"collectTime\",\"pageSize\":2,\"start\":0}}");

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();

        Call call = okHttpClient.newCall(request);
        try {
            Response response = call.execute();
            System.out.println(response.body().string());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        post();
    }
}
