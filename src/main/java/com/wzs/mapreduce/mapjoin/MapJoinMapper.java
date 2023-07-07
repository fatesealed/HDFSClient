package com.wzs.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author FateSealed
 * @date 2023/03/31 11:36
 **/
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> pdMap = new HashMap<>();
    private Text text = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        final URI[] cacheFiles = context.getCacheFiles();
        final Path path = new Path(cacheFiles[0]);
        final FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        final FSDataInputStream open = fileSystem.open(path);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(open, StandardCharsets.UTF_8));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            final String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        final String[] split = value.toString().split("\t");
        final String pname = pdMap.get(split[1]);
        text.set(split[0] + "\t" + pname + "\t" + split[2]);
        context.write(text, NullWritable.get());
    }
}