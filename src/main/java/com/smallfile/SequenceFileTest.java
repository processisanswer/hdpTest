package com.smallfile;

import com.utils.FsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceFileTest {
    /*读取SequenceFile*/
    public static void main(String[] args) throws IOException {
        SequenceFile.Reader reader=null;
        Configuration conf=new Configuration();
        FileSystem fs=FsUtil.getFileSystem();
        //返回SequenceFile.Reader对象
        reader=new SequenceFile.Reader(
                fs,
                new Path("/dir1/seqFile.seq"),
                conf
        );
       /* ReflectionUtils.newInstance(reader.getKeyClass(),conf);
        long position=reader.getPosition();
        Text key=new Text();
        Text value=new Text();

        while(reader.next(key,value)){
            String syncSeen=reader.syncSeen()?"*":"";//替换特殊字符，同步
            System.out.printf("[%s%s]\t%s\t%s\n",position,
                    syncSeen,key,value);
            position=reader.getPosition();//beginning for next record
        }*/
        System.out.println("---------第二种方式----------");
        secondGetWay(fs,
                conf,new Path("/dir1/seqFile.seq"),reader);


    }
    public  static void secondGetWay(
            FileSystem fs,Configuration conf,Path seqFile,
            SequenceFile.Reader reader

    ) throws IOException {
        //Writer内部类用于文件的写操作，假设Key和Vlaue都为Text类型
        SequenceFile.Writer writer= new SequenceFile.Writer(fs,conf,seqFile,
                Text.class,Text.class);
        //通过Writer向文档中写入记录
        writer.append(new Text("key"),new Text("value"));
        IOUtils.closeStream(writer);//关闭write流
        //通过reader从文档中读取记录
        Text key=new Text();
        Text value=new Text();
        while (reader.next(key,value)){
            System.out.println("key:"+key);
            System.out.println("value:"+value);
        }
        IOUtils.closeStream(reader);//关闭reader流
    }

}