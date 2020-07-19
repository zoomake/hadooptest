package com.hadooptest.mr.mongodb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.ArrayList;

public class MongoMapper extends Mapper<Object, BSONObject, Text, IntWritable> {
    //定义输出key的类型
    Text word = new Text();

    @Override
    protected void map(Object key, BSONObject value, Context context)
            throws IOException, InterruptedException {
        if (value.get("Category") != null) {
            ArrayList<BSONObject> gold = (ArrayList<BSONObject>) value.get("gold");
            for (int i = 0; i < gold.size(); i++) {
                String tag_id = "";
                word.set(tag_id);
                context.write(word, new IntWritable(1));
            }
        }
    }
}