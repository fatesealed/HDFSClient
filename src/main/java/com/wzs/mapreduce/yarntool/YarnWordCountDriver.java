package com.wzs.mapreduce.yarntool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * @author FateSealed
 * @date 2023/04/02 15:08
 **/
public class YarnWordCountDriver {
    private static Tool tool;

    public static void main(String[] args) throws Exception {
        final Configuration entries = new Configuration();
        if (args[0].equals("wordcount")) {
            tool = new YarnWordCount();
        } else {
            throw new RuntimeException("No such tool: " + args[0]);
        }
        final int run = ToolRunner.run(entries, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}