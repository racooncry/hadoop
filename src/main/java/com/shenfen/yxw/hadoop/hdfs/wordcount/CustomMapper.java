package com.shenfen.yxw.hadoop.hdfs.wordcount;

public interface CustomMapper {
    /**
     * 读取每一行数据
     * @param line
     * @param customContext
     */
    public void map(String line, CustomContext customContext);
}
