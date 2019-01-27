package com.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 *hadoop1.x
 *使用旧api写单词计数的例子
 */
public class WordCount {
    private static final String INPUT_PATH = "hdfs://192.168.2.128:9000/hello";
    private static final String OUT_PATH = "hdfs://192.168.2.128:9000/out.res";
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            //fs.delete(new Path(OUT_PATH),true);
            JobConf job = new JobConf(conf, WordCount.class);
            job.setJarByClass(WordCount.class);

            FileInputFormat.setInputPaths(job, INPUT_PATH);
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
            JobClient.runJob(job);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] splited = line.split("\t");
            for (String word : splited) {
                output.collect(new Text(word), new LongWritable(1L));
            }
        }
    }

    public static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{

        @Override
        public void reduce(Text key, Iterator<LongWritable> values,
                           OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException {
            long times = 0L ;
            while (values.hasNext()) {
                LongWritable longWritable = (LongWritable) values.next();
                times += longWritable.get();
            }
            output.collect(key, new LongWritable(times));
        }

    }

}