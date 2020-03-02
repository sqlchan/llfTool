package com.sql.hbase.nosleep.mapReduce.topN;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

// 生成随机数：该程序生成了10个文件，每个文件包括一百万个Integer范围的随机数，生成完成后将其复制并上传到虚拟机的Hadoop文件系统HDFS中
public class Num_Generator {
    public static void main(String[] args) {
        FileOutputStream fos;
        OutputStreamWriter osw;
        BufferedWriter bw;
        Random random = new Random();
        String filename = "random_num";

        for (int i = 0; i < 10; i++) {
            String tmp_name = filename+""+i+".txt";
            File file = new File(tmp_name);
            try {
                fos = new FileOutputStream(file);
                osw = new OutputStreamWriter(fos,"UTF-8");
                bw = new BufferedWriter(osw);
                for (int j = 0; j < 1000000; j++) {
                    int rm = random.nextInt();
                    bw.write(rm+"");
                    bw.newLine();
                }
                bw.flush();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(i+":Complete.");
        }
    }
}

