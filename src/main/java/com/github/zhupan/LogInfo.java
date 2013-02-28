package com.github.zhupan;

import org.apache.hadoop.io.Text;

import java.io.Serializable;

/**
 * @author PanosZhu
 */
public class LogInfo implements Serializable {

    private static final String SEPARATOR = "~";

    private long time;

    private long elapsedTime;

    private String resultCode;

    public LogInfo(long time, long elapsedTime, String resultCode) {
        this.time = time;
        this.elapsedTime = elapsedTime;
        this.resultCode = resultCode;
    }

    public long getTime() {
        return time;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public String getResultCode() {
        return resultCode;
    }

    public static Text toText(String[] line) {
        return new Text(line[0] + SEPARATOR + line[1] + SEPARATOR + line[3]);
    }

    public static LogInfo of(Text text) {
        String[] values = text.toString().split(SEPARATOR);
        return new LogInfo(Long.valueOf(values[0]), Long.valueOf(values[1]), values[2]);
    }

    public boolean hasError() {
        return !"200".equals(getResultCode());
    }
}
