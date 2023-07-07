package com.wzs.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/29 18:19
 **/
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取配置信息以及获取job对象
        final Configuration entries = new Configuration();
        final Job instance = Job.getInstance(entries);
        // 2 关联本Driver程序的jar
        instance.setJarByClass(WordCountDriver.class);
        // 3 关联本Mapper和Reducer的jar
        instance.setMapperClass(WordCountMapper.class);
        instance.setReducerClass(WordCountReducer.class);
        // 4 设置Mapper输出的KV类型
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出kv类型
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(instance, new Path(args[0]));
        FileOutputFormat.setOutputPath(instance, new Path(args[1]));
        // 7 提交job
        final boolean b = instance.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}