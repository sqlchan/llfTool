package com.llf.util.string;

import com.alibaba.fastjson.JSON;
import com.llf.util.DTO.Address;
import com.llf.util.DTO.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Struct;

public class LLFStringUtils {
    private static Logger logger = LoggerFactory.getLogger(LLFStringUtils.class);

    public static void main(String[] args) {
        Address address = new Address();
        address.setAdd("he");
        Student student = new Student();
        student.setName("llf");
        student.setAddress(address);

        String json = JSON.toJSONString(student);
        System.out.println(json);

        String s = "{\"address\":{\"add\":\"he\",\"num\":1},\"age\":1,\"name\":\"llf\"}";
        student = JSON.parseObject(s,Student.class);
        json = JSON.toJSONString(student);
        System.out.println(json);
    }
}
