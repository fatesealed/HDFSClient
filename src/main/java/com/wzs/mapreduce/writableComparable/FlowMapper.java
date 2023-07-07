package com.wzs.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.swing.plaf.IconUIResource;
import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/30 13:32
 **/
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean outK = new FlowBean();
    Text outV=new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        final String s = value.toString();
        final String[] split = s.split("\t");
        outV.set(split[0]);
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow(Long.parseLong(split[3]));

        context.write(outK, outV);
    }
}