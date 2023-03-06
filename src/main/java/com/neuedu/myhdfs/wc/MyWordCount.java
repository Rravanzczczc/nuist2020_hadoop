package com.neuedu.myhdfs.wc;

import com.ctc.wstx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MyWordCount {
    private static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

        @Override
        protected void map(LongWritable key,Text value,Mapper<LongWritable, Text,Text, IntWritable>.Context context) throws IOException, InterruptedException{

            //数据清洗
            String line = value.toString();
            //判断是否为空
            if (StringUtils.isBlank(line)){
                return;
            }
            //拆分单词
            StringTokenizer token = new StringTokenizer(line);
            //循环提取单词
            while (token.hasMoreTokens()){
                //提取单词
                String word = token.nextToken();
                //输出
                context.write(new Text(word),new IntWritable(1));

            }
        }


    }

    /**
     * 自定义Reducer处理类，统计单词频率
     */

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException,InterruptedException{
            //统计单词的次数
            int sum = 0;
            for (IntWritable v :values) {
                sum += v.get();
            }
            //输出单词和次数
            context.write(key, new IntWritable(sum));
        }


    }

    public static void main(String[] args) {
        try {
            //判断参数
            if (null== args || args.length !=2){
                return;

            }            // 构造配置对象
            Configuration conf = new Configuration();

            //设置HADOOP集群属性。
            //conf.set("fs.defaults","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义输入目录
            //String input = "/books";
            String input = args[0];
            //定义输出目录
            //String output = "wc_output";
            String output = args[1];

            Path outputPath = new Path(output);
            //查看是否已经存在目录路径，若有则删除
            if (hdfs.exists(outputPath)){
                hdfs.delete(outputPath,true);
            }
            //实例化Job对象
            Job job = Job.getInstance(conf,"word count");
            //设置运行类
            job.setJarByClass(MyWordCount.class);
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            //设置Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置Reducer
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if(flag){
                System.out.println("词频统计~~~~~");
            }

        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }
}
