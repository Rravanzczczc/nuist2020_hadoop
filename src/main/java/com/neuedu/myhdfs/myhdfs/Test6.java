package com.neuedu.myhdfs.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test6 {
    public static void main(String[] args) {

        try {
            // 构造配置对象
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);

            Path dst = new Path("/gg.txt");

            if (!hdfs.exists(dst)){
                System.out.println(dst.getName()+"文件路径不存在");
                return;
            }

            boolean flag = hdfs.delete(dst,true);
            if (flag){
                System.out.println("delete success");
            }
            //定义目标路径
            Path src = new Path("/books/kkk.txt");

            //查看块 的信息
            FileStatus fs = hdfs.getFileStatus(src);
            BlockLocation[] blocks = hdfs.getFileBlockLocations(fs,0,fs.getLen());
            for(BlockLocation b : blocks){
                for (int i =0;i < b.getNames().length;i++){
                    System.out.println(b.getNames()[i]);
                    System.out.println("----------------------------");
                    System.out.println(b.getOffset());
                    System.out.println("----------------------------");
                    System.out.println(b.getLength());
                    System.out.println("----------------------------");
                    System.out.println(b.getHosts()[i]);

                }
                System.out.println("----------------------------");
            }



        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
