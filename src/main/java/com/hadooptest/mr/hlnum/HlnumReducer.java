package com.hadooptest.mr.hlnum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HlnumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable total = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum = 0;
        for(IntWritable i : values){
            sum += i.get();
        }
        total.set(sum);
        context.write(key, total);
    }
}
