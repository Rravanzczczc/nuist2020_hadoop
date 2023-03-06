package com.neuedu.myhdfs.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class S2R {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);

            //定义目标路径
            Path dst = new Path("/sequencefile/s.dat");

            //定义读取器
            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(dst));
            //定义文件名变量，文件内容变量
            IntWritable file = new IntWritable();
            Text content = new Text();
            //循环读取
            while(reader.next(file,content)){
                System.out.println("文件名"+file);
                System.out.println("文件内容为"+content);
            }

            //关闭读取器
            reader.close();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
