package tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Linux {
    public static void main(String[] args) {
        try {
            String[] cmds = {"/bin/sh","-c","netstat -antp | grep 8482 | wc -l"};
            Process pro = Runtime.getRuntime().exec(cmds);
            pro.waitFor();
            InputStream in = pro.getInputStream();
            BufferedReader read = new BufferedReader(new InputStreamReader(in,"utf-8"));
            String line = null;

            while((line = read.readLine())!=null){
                if (line.trim().equals("0")) System.out.println("false");
                else System.out.println("true");
            }
            read.close();
        } catch (InterruptedException e1) {
            System.out.println("occur InterruptedException when exec kill tomcat shell");
        } catch (IOException e2) {
            System.out.println("occur IOException when exec kill tomcat shell");
        }
    }
}
