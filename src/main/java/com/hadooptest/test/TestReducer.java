package com.hadooptest.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//自定义一个reduce类，归并
    public class TestReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //reduce参数中values是一个集合，表示相同单词出现的次数，可能有好几个1，所以要求和
            long sum=0;
            for(LongWritable value:values){
                //统计单词key出现的次数
                sum+=value.get();
            }
            System.out.println(key.toString());
            System.out.println(values.toString());
            //最终统计结果的输出,即<k3,v3>,
            context.write(key,new LongWritable(sum));

        }
    }
