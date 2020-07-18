package com.shenfen.yxw.hadoop.mapreduce.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;


import java.io.IOException;

/**
 * 使用MapReduce统计HDFS上的文件对应的词频
 */
public class WordCounApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME","root");
       // System.setProperty("hadoop.home.dir", "E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2");
        // 加载库文件
        System.load("E:\\all\\coderme\\hadoop\\hadoop-2.6.0-cdh5.16.2\\bin\\hadoop.dll");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop001:8020");
        // 创建一个job
        Job job = Job.getInstance(configuration);

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

        // 作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }
}
