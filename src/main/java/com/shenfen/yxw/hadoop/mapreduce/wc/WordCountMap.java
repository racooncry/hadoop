package com.shenfen.yxw.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Map任务读数据的key类型，offset，是每行数据的起始偏移量，Long
 * VALUEIN：Map任务读数据的value类型，其实就是一行行的字符串，String
 * <p>
 * hello world welcome
 * hello welcome
 * <p>
 * KEYOUT: map方法自定义实现输出的key类型, String
 * VALUEOUT: map方法自定义实现输出的value类型, Integer
 * <p>
 * 词频统计: 相同单词的次数   (word,1)
 * <p>
 * <p>
 * Long,String ,String ,Integer
 * Hadoop 自定义类型：序列化和反序列化
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分隔符拆开
        String[] split = value.toString().split("\t");

        for (String word :
                split) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
