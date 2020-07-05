package com.shenfen.yxw.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * (hello,1)  (word,1)
     * (hello,1)  (word,1)
     * (hello,1)  (word,1)
     * (welcome,1)
     * <p>
     * map的输出到reduce，是按照相同的key分发到一个reduce上执行
     * reducer1：(hello,1) (hello,1) (hello,1) ==>(hello,<1,1,1>)
     * reducer2：(hello,1) (hello,1) (hello,1) ==>(hello,<1,1,1>)
     * reducer3：(welcome,1) ==>(welcome,<1>)
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     * Recude和Mapper中使用到了什么设计模式：模板
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        // <1,1,1>
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
