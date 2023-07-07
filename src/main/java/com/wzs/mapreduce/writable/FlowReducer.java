package com.wzs.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author FateSealed
 * @date 2023/03/30 13:53
 **/
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long upSum = 0;
        long downSum = 0;
        for (FlowBean value : values) {
            upSum += value.getUpFlow();
            downSum += value.getDownFlow();
        }
        outV.setUpFlow(upSum);
        outV.setDownFlow(downSum);
        outV.setSumFlow();
        context.write(key, outV);
    }
}