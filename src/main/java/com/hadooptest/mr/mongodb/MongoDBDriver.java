package com.hadooptest.mr.mongodb;

import com.mongodb.hadoop.MongoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MongoDBDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        conf.set("mongo.input.uri", "mongodb://10.10.192.26:30000,10.10.192.27:30000,10.10.192.28:30000/rms-data.newsblock");

        Job job = Job.getInstance(conf, "Mongo Connection");
        job.setJarByClass(MongoDBDriver.class);
        job.setMapperClass(MongoMapper.class);
        job.setCombinerClass(MongoReducer.class);
        job.setReducerClass(MongoReducer.class);

        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        conf.set("fs.defaultFS", "hdfs://###.186.240:8020");
        conf.set("dfs.client.use.datanode.hostname", "true");
        Path outputPath = new Path("hdfs://###.186.240:8020/mongo/");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        int waitForCompletion = job.waitForCompletion(true) ? 0 : 1;
        System.exit(waitForCompletion);
    }

//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        int exitCode = ToolRunner.run(conf, new MongoConnectTest(), args);
//        System.exit(exitCode);
//    }
}
