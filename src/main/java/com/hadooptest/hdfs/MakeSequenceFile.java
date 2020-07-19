package com.hadooptest.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MakeSequenceFile {
    private static Logger log = Logger.getLogger(MakeSequenceFile.class);
    //将目标目录的所有文件以文件名为key，内容为value放入SequenceFile中
    //第一个参数是需要打包的目录，第二个参数生成的文件路径和名称
    private static void combineToSequenceFile(String sourceDir, String destFile) throws IOException {
        //1.创建 Configuration
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS", "hdfs://106.14.186.240:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");//重点配置 否则本地调试返回阿里云内网IP
        //打印日志
        BasicConfigurator.configure();

        List<String> files = getFiles(sourceDir, configuration);
        FileSystem fs = FileSystem.get(configuration);
        Path destPath = new Path(destFile);
        if (fs.exists(destPath)) {
            fs.delete(destPath, true);
        }

        FSDataInputStream in = null;

        Text key = new Text();
        BytesWritable value = new BytesWritable();

        byte[] buff = new byte[4096];
        SequenceFile.Writer writer = null;

        SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(new Path(destFile));
        SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option option4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        try {
            writer = SequenceFile.createWriter(configuration, option1, option2, option3, option4);
            for (int i = 0; i < files.size(); i++) {
                Path path = new Path(files.get(i).toString());
                System.out.println("读取文件：" + path.toString());
                key = new Text(files.get(i).toString());
                in = fs.open(path);
//                只能处理小文件，int最大只能表示到1个G的大小，实际上大文件放入SequenceFile也没有意义
                int length = (int) fs.getFileStatus(path).getLen();
                byte[] bytes = new byte[length];
//                read最多只能读取65536的大小
                int readLength = in.read(buff);
                int offset = 0;
                while (readLength > 0) {
                    System.arraycopy(buff, 0, bytes, offset, readLength);
                    offset += readLength;
                    readLength = in.read(buff);
                }
                System.out.println("file length:" + length + ",read length:" + offset);
                value = new BytesWritable(bytes);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value.getLength());
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(writer);
            IOUtils.closeStream(fs);
        }
    }

    private static List<String> getFiles(String dir,Configuration conf) throws IOException {
        Path path = new Path(dir);
        FileSystem fs = null;
        List<String> filelist = new ArrayList<>();
        try {
            fs = FileSystem.get(conf);

            //对单个文件或目录下所有文件和目录
            FileStatus[] fileStatuses = fs.listStatus(path);

            for (FileStatus fileStatus : fileStatuses) {
                //递归查找子目录
                if (fileStatus.isDirectory()) {
                    filelist.addAll(getFiles(fileStatus.getPath().toString(),conf));
                } else {
                    filelist.add(fileStatus.getPath().toString());
                }
            }
            return filelist;
        } finally {
            IOUtils.closeStream(fs);
        }
    }

    //将combineToSequenceFile生成的文件分解成原文件。
    private static void extractCombineSequenceFile( String sourceFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://106.14.186.240:8020");
        conf.set("dfs.client.use.datanode.hostname", "true");//重点配置 否则本地调试返回阿里云内网IP
        //打印日志
        BasicConfigurator.configure();

        Path sourcePath = new Path(sourceFile);

        SequenceFile.Reader reader = null;
        SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(sourcePath);

        Writable key = null;
        Writable value = null;
//        Text key = null;
//        BytesWritable value = null;

        FileSystem fs = FileSystem.get(conf);
        try {
            reader = new SequenceFile.Reader(conf, option1);
            key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            //在知道key和value的明确类型的情况下，可以直接用其类型
//            key = ReflectionUtils.newInstance(Text.class, conf);
//            value =  ReflectionUtils.newInstance(BytesWritable.class, conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                FSDataOutputStream out = fs.create(new Path(key.toString()), true);
                //文件头会多出4个字节，用来标识长度，而本例中原文件头是没有长度的，所以不能用这个方式写入流
//                value.write(out);
                out.write(((BytesWritable)value).getBytes(),0,((BytesWritable)value).getLength());

                //                out.write(value.getBytes(),0,value.getLength());
                System.out.printf("[%s]\t%s\t%s\n", position, key, out.getPos());
                out.close();
                position = reader.getPosition();
            }
        } finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(fs);
        }
    }

    public static void main(String[] args) throws IOException {
        String in = "hdfs://106.14.186.240:8020/hlnumlog/input/";
        String out = "hdfs://106.14.186.240:8020/hlnumlog/output2/";
//        combineToSequenceFile(in, out);

        extractCombineSequenceFile(out);
    }
}
