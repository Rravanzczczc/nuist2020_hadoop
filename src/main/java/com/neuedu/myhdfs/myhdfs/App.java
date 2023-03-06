package com.neuedu.myhdfs.myhdfs;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        try {

            // 构造配置对象
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义写入文件的内容
            String[] data = {"stormviile","link","The Tarnished"};

            //远程文件位置
            Path dst = new Path("/test1/gg.txt");

            //创建FSDataOouputStream对象
            FSDataOutputStream out = hdfs.create(dst,true);



            for (String line : data){
                //写入内容
                out.writeUTF(line);
                //强制将缓冲写入 目标
                out.flush();

            }
            //关闭
            out.close();
            if (hdfs.exists(dst)){
                System.out.println("success");
            }

        }catch (IOException e){
            e.printStackTrace();
        }



    }
}
