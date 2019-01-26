package com.smallfile;


import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import com.utils.FsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapFileTest {
    //MapFile是基于SequenceFile的，是排序后的SequenceFile，由两部分组成
    //Data和index组成
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        FileSystem fs= FsUtil.getFileSystem();
        Path mapFile=new Path("/dir1/mapFile1.map");
        //Writer内部类用于文件的写操作，假设key和value都是Text类型
        MapFile.Writer writer=new MapFile.Writer(
                conf,fs,"/dir1/mapFile1.map", Text.class,Text.class
        );
        //通过writer向文档中写入记录
        writer.append(new Text("key"),new Text("value"));
        IOUtils.closeStream(writer);//关闭write流
        //Reader内部类用于文件的读取操作
        MapFile.Reader reader=new MapFile.Reader(
                fs,mapFile.toString(),conf
        );
        //通过Reader从文档中读取记录
        Text key=new Text();
        Text value=new Text();
        while(reader.next(key,value)){
            System.out.println("key:"+key);
            System.out.println("value:"+value);
        }
        IOUtils.closeStream(reader);//关闭read流

    }
}