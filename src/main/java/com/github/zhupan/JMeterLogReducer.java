package com.github.zhupan;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * @author PanosZhu
 */
public class JMeterLogReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Long totalValue = 0L;
        Long errorCount = 0L;
        Long minTime = Long.MAX_VALUE;
        Long maxTime = Long.MIN_VALUE;
        List<Integer> list = new ArrayList<Integer>();
        for (Text text : values) {
            LogInfo log = LogInfo.of(text);
            if (log == null) {
                continue;
            }
            minTime = Math.min(minTime, log.getTime());
            maxTime = Math.max(maxTime, log.getTime());
            list.add(log.getElapsedTime());
            totalValue += log.getElapsedTime();
            if (log.hasError()) {
                errorCount += 1;
            }
        }
        Integer[] sortedValues = SortUtils.sort(list.toArray(new Integer[]{}));
        Integer samples = sortedValues.length;
        context.write(new Text(key.toString() + " Total Samples"), new Text(samples.toString()));
        context.write(new Text("Average Value"), new Text(String.valueOf(totalValue / samples)));
        context.write(new Text("Median Value"), new Text(String.valueOf(sortedValues[samples / 2])));
        context.write(new Text("90% Line Value"), new Text(sortedValues[Double.valueOf(samples * 0.9).intValue()].toString()));
        context.write(new Text("Min Value"), new Text(sortedValues[0].toString()));
        context.write(new Text("Max Value"), new Text(sortedValues[samples - 1].toString()));
        context.write(new Text("Error%"), new Text(BigDecimal.valueOf(errorCount * 1.0 / samples * 100).setScale(3, BigDecimal.ROUND_HALF_UP) + "%"));
        context.write(new Text("Throughput(TPS)"), new Text(String.valueOf(samples / ((maxTime - minTime) / 1000))));
        context.write(new Text("Start Time"), new Text(new Date(minTime).toString()));
        context.write(new Text("End Time"), new Text(new Date(maxTime).toString()));
    }

}
