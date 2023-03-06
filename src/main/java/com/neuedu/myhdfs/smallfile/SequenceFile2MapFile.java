package com.neuedu.myhdfs.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import javax.sound.midi.Sequence;
import java.io.IOException;

public class SequenceFile2MapFile {
    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //CONF.set("fs.defaults","hdfs://master:9000")
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义目标路径
            Path dst = new Path("/sequencefile");
            Path src = new Path(dst, MapFile.DATA_FILE_NAME);

            //读取key和value的值，若已知可忽略
            //定义读取器，读取key和value的数据类型
            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(src));
            Class keyClass = reader.getKeyClass();
            Class valueClass = reader.getValueClass();
            reader.close();

            //
            long r = MapFile.fix(hdfs ,dst,keyClass,valueClass,false,conf);
            System.out.println("转换成功" +
                    r);

        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            throw new RuntimeException();
        }
    }
}
