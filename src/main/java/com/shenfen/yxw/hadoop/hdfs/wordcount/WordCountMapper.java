package com.shenfen.yxw.hadoop.hdfs.wordcount;

public class WordCountMapper implements CustomMapper {
    @Override
    public void map(String line, CustomContext customContext) {
        String[] words = line.toLowerCase().split("\t");
        for (String word : words) {
            Object value = customContext.get(word);
            if (value == null) {
                customContext.write(word, 1);
            } else {
                int i = Integer.parseInt(value.toString());
                customContext.write(word, i + 1);
            }
        }
    }
}
