package com.wzs.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/31 10:44
 **/
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String name;
    private Text outK = new Text();
    private TableBean outV = new TableBean();


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        final FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        final String s = value.toString();
        if (name.contains("order")) {
            final String[] split = s.split("\t");
            outK.set(split[1]);
            //封装outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {
            String[] split = s.split("\t");
            //封装outK
            outK.set(split[0]);
            //封装outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }
        context.write(outK, outV);
    }
}