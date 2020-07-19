package com.hadooptest.mr.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordCountTest {
    //封装MapReduce作业的所有信息
    public static void main(String[] args) throws Exception{
        //1.创建 Configuration
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS", "hdfs://106.14.186.240:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");//重点配置 否则本地调试返回阿里云内网IP

        //打印日志
        BasicConfigurator.configure();

        //创建Job之前，准备清理已经存在的输出目录
        Path outputPath= new Path("hdfs://106.14.186.240:8020/test/output/");
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
            System.out.println("输出文件夹存在且已被删除");
        }

        //2.创建job，通过getInstance 拿到一个实例
        Job job= Job.getInstance(configuration,"wordCount");
        //3.设置Job的处理类
        job.setJarByClass(WordCountTest.class);
        //4.设置作业处理的输入路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://106.14.186.240:8020/test/input/"));

        //************5.设置map相关参数
         //设置map的处理类
        job.setMapperClass(TestMaper.class);
         //设置map输出参数的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //************6.设置reduce相关参数
        //设置reduce的处理类
        job.setReducerClass(TestReducer.class);
        //设置reduce输出参数的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //7.设置作业的输出路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://106.14.186.240:8020/test/output/"));
        //8.提交结果
         //参数true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        boolean result=job.waitForCompletion(true);      
        System.exit(result? 0: 1);

    }
}
