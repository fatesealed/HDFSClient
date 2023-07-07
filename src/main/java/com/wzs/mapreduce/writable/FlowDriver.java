package com.wzs.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/30 14:03
 **/
public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration entries = new Configuration();
        final Job job = Job.getInstance(entries);
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
//        job.setPartitionerClass(ProvincePartitioner.class);
//        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job, new Path("D:\\Desktop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Desktop\\output"));
        final boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}