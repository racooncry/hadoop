package com.shenfen.yxw.hadoop.mapeduce.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 如果不想在文件中输出key，可以使用NullWritable，继承时的声明和启动类的Reducer输出都要记得改一下哦
 */
public class AccessReducer extends Reducer<Text, Access, NullWritable, Access> {
    /**
     * @param key     手机号
     * @param values  <Access,Access>
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;
        for (Access access :
                values) {
            ups += access.getUp();
            downs += access.getDown();
        }
        context.write(NullWritable.get(), new Access(key.toString(), ups, downs));
    }
}
