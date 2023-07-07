package com.wzs.mapreduce.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/31 12:33
 **/
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private boolean check(String s, Context context) {
        final String[] s1 = s.split(" ");
        return s1.length > 11;
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        final String s = value.toString();
        final boolean check = check(s, context);
        if (!check) {
            return;
        }
        context.write(value, NullWritable.get());
    }
}