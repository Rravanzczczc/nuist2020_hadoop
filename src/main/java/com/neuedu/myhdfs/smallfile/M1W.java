package com.neuedu.myhdfs.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author zzz
 *有些问题没有解决
 *
 */
public class M1W {
    public static void main(String[] args) {
        //定义文件内容，模拟四个文件
        String[] Data = {
                "hello world",
                "i try to get something",
                "yes my lord",
                "pardon me?"
        };

        try {
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")

            //定义目标路径
            Path dst = new Path("/mapfile/m");
            //定义写入器
            MapFile.Writer writer = new MapFile.Writer(conf,dst,
                    MapFile.Writer.keyClass(IntWritable.class),
                    MapFile.Writer.valueClass(Text.class),
                    MapFile.Writer.compression(SequenceFile.CompressionType.NONE));
            //N个小文件合并
            for (int i = 1; i<= Data.length;i++){
                writer.append(new IntWritable(i),new Text(Data[i-1]));

            }
            writer.close();
            //153页开始

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
