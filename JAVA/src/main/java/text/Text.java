package text;

import java.io.*;

public class Text {
    public static void main(String[] args) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(new File("D:\\code\\llf\\llfTool\\JAVA\\src\\main\\resources\\a.txt")));
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("D:\\code\\llf\\llfTool\\JAVA\\src\\main\\resources\\b.txt")));
            BufferedWriter writer1 = new BufferedWriter(new FileWriter(new File("D:\\code\\llf\\llfTool\\JAVA\\src\\main\\resources\\c.txt")));
            BufferedWriter writer2 = new BufferedWriter(new FileWriter(new File("D:\\code\\llf\\llfTool\\JAVA\\src\\main\\resources\\d.txt")))){
            String line = null;
            String line1 = null;
            String line2 = null;
            while (null != (line = reader.readLine())){
                if (line.startsWith("0x0")){
                    writer.write(line);
                    writer.write(".description=");
                    line1 = reader.readLine();
                    writer.write(line1);
                    writer.newLine();
                }
                line2 = reader.readLine();
                writer2.write(line);
                writer2.write(".suggestion=");
                writer2.write(line2);
                writer2.newLine();
            }
        }
    }
}
