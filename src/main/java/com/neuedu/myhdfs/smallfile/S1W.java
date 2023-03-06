package com.neuedu.myhdfs.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class S1W {

    public static void main(String[] args) {

        String[] data ={
                "data1",
                "data2",
                "data3",
                "data4",
        };

        try {
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);

            //定义目标路径
            Path dst = new Path("/sequencefile/s.dat");
            SequenceFile.Writer writer =  SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(dst),
                    SequenceFile.Writer.keyClass(IntWritable.class),
                    SequenceFile.Writer.valueClass(Text.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

            //N个小文件合并，文件名是整形，文件内容是文本
            for (int i = 1 ; i <= data.length;i++){
                writer.append(new IntWritable(i),
                        new Text(data[i-1]));
            }
            //关闭好习惯
            writer.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
