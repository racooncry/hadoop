package com.shenfen.yxw.hadoop.mapeduce.access;

import com.shenfen.yxw.hadoop.mapreduce.wc.WordCounApp;
import com.shenfen.yxw.hadoop.mapreduce.wc.WordCountMap;
import com.shenfen.yxw.hadoop.mapreduce.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class AccessLocalApp {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        String hadoopUrl = "hdfs://192.168.37.142:8020";
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        configuration.set("fs.defaultFS", hadoopUrl);
        // 创建一个job
       // Job job = Job.getInstance(configuration);
          Job job = Job.getInstance();
        // 设置job的主类
        job.setJarByClass(AccessLocalApp.class);

        // 设置map和reduce的处理类
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        // Reduce输出key和value的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);



        // 设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        // 设置Reducer个数
        job.setNumReduceTasks(3);


        String inputPathStr = "access/input";
        String outPathStr = "access/output";

        FileSystem fileSystem = FileSystem.get(new URI(hadoopUrl), configuration, "root");
        Path outPath = new Path(outPathStr);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        // 作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path(inputPathStr));
        FileOutputFormat.setOutputPath(job, outPath);

        // 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }
}
