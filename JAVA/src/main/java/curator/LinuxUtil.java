package curator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LinuxUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(LinuxUtil.class);
    /**
     * 执行shell脚本
     *
     * @param path
     * @param args
     * @return
     */
    public static boolean execScript(String path, String... args) {
        List<String> shellStr = new ArrayList<>();
        shellStr.add("/bin/sh");
        shellStr.add(path);
        for (String arg : args) {
            shellStr.add(arg);
        }
        int size = shellStr.size();
        return execShell(shellStr.toArray(new String[size]));
    }

    /**
     * shell 执行命令
     *
     * @param shellStr
     * @return
     */
    public static boolean execShell(String[] shellStr) {
        int exitCode = -1;
        try {

            LOGGER.info("run shell :{}",Arrays.toString(shellStr));
            Process process = Runtime.getRuntime().exec(shellStr);
            process.waitFor();
            exitCode = process.exitValue();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.debug(String.valueOf(exitCode));
        return  "0".equals(exitCode);
    }

}


