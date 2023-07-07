package com.wzs.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/30 13:32
 **/
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        final String s = value.toString();
        final String[] split = s.split("\t");
        int n = split.length;
        outK.set(split[1]);
        outV.setUpFlow(Long.parseLong(split[n - 3]));
        outV.setDownFlow(Long.parseLong(split[n - 2]));
        outV.setSumFlow();
        context.write(outK, outV);
    }
}