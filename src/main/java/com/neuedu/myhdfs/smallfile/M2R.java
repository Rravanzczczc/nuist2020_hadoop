package com.neuedu.myhdfs.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class M2R {
    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")

            //定义目标路径
            Path dst = new Path("/mapfile/m");
            //定义读取器
            MapFile.Reader reader = new MapFile.Reader(dst,conf);

            //定义文件名和内容的变量
            IntWritable fileName = new IntWritable();
            Text Content = new Text();
            //循环读取所有的小文件
            while(reader.next(fileName,Content)){
                //输入文件信息
                System.out.println("文件名"+fileName);
                System.out.println("文件内容"+Content);
            }

            reader.close();

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
