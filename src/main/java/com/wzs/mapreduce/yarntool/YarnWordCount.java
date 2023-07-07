package com.wzs.mapreduce.yarntool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Random;

/**
 * @author FateSealed
 * @date 2023/04/02 14:34
 **/
public class YarnWordCount implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        final Job job = Job.getInstance(conf);
        job.setJarByClass(YarnWordCount.class);
        job.setMapperClass(YarnWordCountMapper.class);
        job.setReducerClass(YarnWordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class YarnWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outK = new Text();
        private final IntWritable outV = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            final String s = value.toString();
            final String[] s1 = s.split(" ");
            for (String s2 : s1) {
                outK.set(s2);
                context.write(outK, outV);
            }
        }
    }

    public static class YarnWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outV = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outV.set(sum);
            context.write(key, outV);
        }
    }
}