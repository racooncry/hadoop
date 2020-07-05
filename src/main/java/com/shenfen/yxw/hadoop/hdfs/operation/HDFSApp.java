package com.shenfen.yxw.hadoop.hdfs.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;
import java.net.URI;

public class HDFSApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "E:\\all\\coderme\\hadoop");

        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:8020"), configuration, "root");
        write(fs);
    }

    public static void mkdir(FileSystem fs) throws IOException {

        Path path = new Path("/hdfsapi/test");
        boolean mkdirs = fs.mkdirs(path);
        System.out.println(mkdirs);
    }

    public static void write(FileSystem fs) throws IOException {
        Path path = new Path("/hdfsapi/test/hello.txt");
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.writeUTF("hello world name");
        outputStream.flush();
        outputStream.close();
    }
}
