package com.neuedu.myhdfs.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 按块读取
 */
public class Test3 {
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
            byte[] block = new byte[16];
            //查询块的大小
            int size = in.available();
            //计算块的数量
            int blockNum = size/block.length;
            //计算剩余块的大小哦
            int leftblock = size%block.length;
            if ( blockNum != 0) {
                for (int i = 1; i < blockNum; i++) {
                    //读取内容，并输入
                    int b = in.read(block);
                    //将字节数组转换为字符串
                    String Bline = new String(block);
                    //输出
                    System.out.println(Bline);

                }
            }
            if (leftblock > 0) {
                byte[] block2 = new byte[leftblock];

                String LBline = new String(block2);

                System.out.println(LBline);

            }


            System.out.println("success");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
