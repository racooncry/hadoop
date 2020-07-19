package com.shenfen.yxw.hadoop.mapreduce.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用MapReduce统计HDFS上的文件对应的词频
 */
public class WordCounApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    //  System.setProperty("hadoop.home.dir", "E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2");
       // System.load("E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2\\bin\\hadoop.dll");
        String hadoopUrl = "hdfs://192.168.37.142:8020";
        System.setProperty("HADOOP_USER_NAME", "root");
        // System.setProperty("hadoop.home.dir", "E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2");
      //  System.setProperty("hadoop.home.dir", "E:\\all\\coderme\\hadoop");

        // 加载库文件
//        System.load("E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2\\bin\\hadoop.dll");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        configuration.set("fs.defaultFS", hadoopUrl);
        // 创建一个job
        Job job = Job.getInstance(configuration);
       //  Job job = Job.getInstance();
        // 设置job的主类
        job.setJarByClass(WordCounApp.class);

        // 设置map和reduce的处理类
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReducer.class);

        // Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = FileSystem.get(new URI(hadoopUrl), configuration, "root");
   Path outPath = new Path("/wordcount/output");
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        // 作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, outPath);

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }
}
