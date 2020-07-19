package com.hadooptest.mr.mongodb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MongoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //定义输出key和value的类型
    IntWritable value = new IntWritable();
    Text word = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //统计map端输出的数据
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        value.set(sum);
        //String substring = key.toString().substring(2, key.toString().length());
        //word.set(substring);
        word.set(key.toString());
        System.out.println(word + ":" + value);
        context.write(word, value);
    }
}

