package com.neuedu.myhdfs.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Test4 {

    public static void main(String[] args) {

        try {
            // 构造配置对象
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);

            Path dst = new Path("/books/ttt.txt");

            if (!hdfs.exists(dst)){
                System.out.println(dst.getName()+"文件路径不存在");
                return;
            }
            //创建FSDataInputStream对象
            FSDataInputStream In = hdfs.open(dst);

            // 创建BUFFERREADER对象
            BufferedReader reader = new BufferedReader( new InputStreamReader(In));

            //
            String line = reader.readLine();
            while (line != null){
                //输出内容
                System.out.println(line);
                //继续读取下一行文本
                line = reader.readLine();
            }
            reader.close();
            In.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
