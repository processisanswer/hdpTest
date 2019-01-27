package com.WordCount.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ÔÚkey valueÒ»¶¨ÒªÓÃhadoop Long(LongWritable),String(Text)
 * @author Administrator
 *
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        //super.map(key, value, context);
        String lines=value.toString();
        String[] values= lines.split("\t");
        for (String word : values) {
            //Êä³ö¼üÖµ¶Ô
            context.write(new Text(word), new IntWritable(1));
        }

    }
}