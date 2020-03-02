package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JsonTestMain {
    public static void main(String[] args) {
        JsonTest jsonTest = new JsonTest();
        jsonTest.setProjectID(111l);
        JSONObject jsonObject = (JSONObject) JSON.toJSON(jsonTest);
        String id = jsonObject.getString("project_id.idd");
        System.out.println(id);
        System.out.println(jsonObject);
    }
}
