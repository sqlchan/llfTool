package util;

import com.alibaba.fastjson.annotation.JSONField;

public class JsonTest {
    @JSONField(name="project_id.idd")
    private Long ProjectID;

    public JsonTest() {
    }

    public Long getProjectID() {
        return ProjectID;
    }

    public void setProjectID(Long projectID) {
        ProjectID = projectID;
    }
}
