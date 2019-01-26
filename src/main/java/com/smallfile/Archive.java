package com.smallfile;

import com.sun.tools.doclets.formats.html.SourceToHTMLConverter;
import com.utils.FsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;

public class Archive {
    //2 使用hadoop archive HAR文件，缓解大量小文件消耗namenode内存的问题
    //3 SequenceFile 工作中使用block 压缩
    public static void main(String[] args) throws IOException {
        /**
         * 写入SequenceFile文件
         */
        Configuration configuration=new Configuration();
        FileSystem fileSystem= FsUtil.getFileSystem();
        FileStatus[] fileStatuses=fileSystem.listStatus(new Path("/dir1"));
        Text key=new Text();
        Text value=new Text();
        Path outpath=new Path("/dir1/seqFile.seq");
        SequenceFile.Writer writer=SequenceFile.createWriter(fileSystem,configuration,
                outpath,key.getClass(),value.getClass());
        InputStream in=null;
        byte[] buffer=null;
        for(int i=0;i< fileStatuses.length;i++){
            key.set(fileStatuses[i].getPath().getName());
            in= fileSystem.open(fileStatuses[i].getPath());
            buffer=new byte[(int)fileStatuses[i].getLen()];
            IOUtils.readFully(in,buffer,0,buffer.length);
            value.set(buffer);
            IOUtils.closeStream(in);
            System.out.println(key.toString()+"\n"+value.toString());
            writer.append(key,value);

        }
        IOUtils.closeStream(writer);
    }



}