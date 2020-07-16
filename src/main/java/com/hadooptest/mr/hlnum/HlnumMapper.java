package com.hadooptest.mr.hlnum;

import com.hadooptest.mr.model.HlnumModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1.第几行数据
 * 2.该行的文本信息
 * 3.输出key类型
 * 4.输出value类型
 */
public class HlnumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    HlnumModel hlnum = new HlnumModel();
    Text request_method = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        hlnum = HlnumModel.parse(value.toString());
        if(hlnum.getIs_validate() && hlnum.getRequest_method().contains("url")){
            request_method.set(hlnum.getRequest_method());
            context.write(request_method, new IntWritable(1));
        }
    }
}
