package com.wzs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author FateSealed
 * @date 2023/03/29 12:24
 **/
public class HdfsClient {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException {
        final URI uri = new URI("hdfs://hadoop102:8020");
        final Configuration entries = new Configuration();
        fileSystem = FileSystem.get(uri, entries);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        fileSystem.mkdirs(new Path("xiyou/huaguoshan/"));
    }

    //上传
    @Test
    public void testPut() throws IOException {
        fileSystem.copyFromLocalFile(false, false, new Path("D:\\Desktop\\a.txt"), new Path("/xiyou"));
    }

    //下载
    public void allTest() throws IOException {
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fileSystem.copyToLocalFile(false, new Path("/xiyou/a.txt"), new Path("d:\\desktop"), true);
        // 2 修改文件名称
        fileSystem.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("/xiyou/huaguoshan/meihouwang.txt"));
        // 2 执行删除 参数二是否递归删除
        fileSystem.delete(new Path("/xiyou"), true);
        //获取所有文件信息
        final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            final LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        //判断是文件还是文件夹
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

    }
    //判断文件还是文件夹

}

