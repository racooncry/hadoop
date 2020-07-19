package com.shenfen.yxw.hadoop.hdfs.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HadoopWrodcount {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
      //  System.setProperty("hadoop.home.dir", "E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2");

        // 1、输入
        Path input = new Path("/hdfsapi/test/hello.txt");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration,"root");

        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input, false);

        WordCountMapper mapper = new WordCountMapper();
        CustomContext customContext = new CustomContext();
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();

            FSDataInputStream in = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                // 词频处理
                System.out.println(line);
                mapper.map(line, customContext);
            }
            reader.close();
            in.close();

        }

        // 将处理结果缓存起来

        Map<Object, Object> contextMap = customContext.getCacheMap();

        // 4、输出
        Path output = new Path("/hdfsapi/output/");
        FSDataOutputStream outputStream = fileSystem.create(new Path(output, new Path("wc.out")));
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();

        for (Map.Entry<Object, Object> entry : entries) {
            System.out.println(entry.getKey().toString() + " \t" + entry.getValue() + " \t");
            outputStream.write((entry.getKey().toString() + " \t" + entry.getValue() + " \t").getBytes());

        }
        outputStream.close();
        fileSystem.close();
        System.out.println("HDFS  统计词频运行成功");
    }
}
