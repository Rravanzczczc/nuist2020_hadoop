package com.neuedu.myhdfs.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *  字节读取
 */

public class Test2 {
    public static void main(String[] args) {

        try {
            // 构造配置对象
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);

            Path dst = new Path("/test1/gg.txt");

            if (!hdfs.exists(dst)){
                return;
            }
            //创建FSDataInputStream对象
            FSDataInputStream in = hdfs.open(dst);
            //循环读取内容，
            int b = in.read();
            while (b!=-1){

                System.out.println((char)b);

                b = in.read();
            }
            System.out.println("success");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
