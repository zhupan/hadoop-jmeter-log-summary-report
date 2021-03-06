package com.github.zhupan;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author PanosZhu
 */
public class JMeterLogMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",", 10);
        if (!validateContent(line)) {
            System.out.println("Line length is " + line.length);
        }
        String outputKey = line[2];
        context.write(new Text(outputKey), LogInfo.toText(line));
    }

    private boolean validateContent(String[] line) {
        if (line.length != 10) {
            return false;
        }
        return true;
    }
}
