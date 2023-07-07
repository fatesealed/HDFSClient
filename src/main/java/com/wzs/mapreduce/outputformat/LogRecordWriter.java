package com.wzs.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author FateSealed
 * @date 2023/03/30 19:57
 **/
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream fsDataOutputStream;
    private FSDataOutputStream fsDataOutputStream1;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        //创建两条流
        try {
            final FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
            fsDataOutputStream = fileSystem.create(new Path("D:\\Desktop\\output\\baidu.log"));
            fsDataOutputStream1 = fileSystem.create(new Path("D:\\Desktop\\output\\other.log"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //具体写
        final String s = text.toString();
        if (s.contains("a")) {
            fsDataOutputStream.writeBytes(s + "\n");
        } else {
            fsDataOutputStream1.writeBytes(s + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(fsDataOutputStream1);
    }
}